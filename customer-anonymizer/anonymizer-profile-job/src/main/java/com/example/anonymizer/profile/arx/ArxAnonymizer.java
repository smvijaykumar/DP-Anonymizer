package com.example.anonymizer.profile.arx;

import org.deidentifier.arx.ARXAnonymizer;
import org.deidentifier.arx.ARXConfiguration;
import org.deidentifier.arx.ARXLattice.ARXNode;
import org.deidentifier.arx.ARXPopulationModel;
import org.deidentifier.arx.ARXResult;
import org.deidentifier.arx.AttributeType;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.Data;
import org.deidentifier.arx.Data.DefaultData;
import org.deidentifier.arx.DataDefinition;
import org.deidentifier.arx.DataHandle;
import org.deidentifier.arx.criteria.DistinctLDiversity;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.risk.RiskModelSampleRisks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Runs ARX over the full customer snapshot. Rows are passed without identifiers; the caller keeps a
 * parallel list of tokens and re-attaches them by row index (ARX keeps input row order in its output).
 */
public final class ArxAnonymizer {
    private static final Logger LOG = LoggerFactory.getLogger(ArxAnonymizer.class);

    public record Params(int k, int l, double suppressionLimit) {
    }

    public record Outcome(List<String[]> rows, boolean[] suppressed, Map<String, Integer> levels,
                          double highestRisk, double averageRisk, long suppressedCount, boolean usedPinnedLevels) {
    }

    /**
     * @param rows   one array per customer: quasi-identifier leaves in {@code qiNames} order, then
     *               sensitive values in {@code sensitiveNames} order
     * @param pinned previous generalization levels to try first, or null
     */
    public Outcome anonymize(List<String> qiNames, List<String> sensitiveNames, List<String[]> rows,
                             Map<String, Hierarchy> hierarchies, Params params, Map<String, Integer> pinned)
            throws IOException {
        if (pinned != null && pinned.keySet().containsAll(qiNames)) {
            Optional<Outcome> withPinned = attempt(qiNames, sensitiveNames, rows, hierarchies, params, pinned);
            if (withPinned.isPresent()) {
                return withPinned.get();
            }
            LOG.warn("Previous generalization levels no longer satisfy the privacy models; searching for a new optimum");
        }
        return attempt(qiNames, sensitiveNames, rows, hierarchies, params, null)
                .orElseThrow(() -> new IllegalStateException("ARX found no transformation satisfying k=" + params.k()
                        + ", l=" + params.l() + " within suppression limit " + params.suppressionLimit()));
    }

    private Optional<Outcome> attempt(List<String> qi, List<String> sensitive, List<String[]> rows,
                                      Map<String, Hierarchy> hierarchies, Params p, Map<String, Integer> pinned)
            throws IOException {
        String[] header = new String[qi.size() + sensitive.size()];
        for (int i = 0; i < qi.size(); i++) {
            header[i] = qi.get(i);
        }
        for (int j = 0; j < sensitive.size(); j++) {
            header[qi.size() + j] = sensitive.get(j);
        }

        DefaultData data = Data.create();
        data.add(header);
        for (String[] r : rows) {
            data.add(r);
        }
        DataDefinition def = data.getDefinition();
        for (String q : qi) {
            def.setAttributeType(q, hierarchies.get(q));
            if (pinned != null) {
                int level = pinned.get(q);
                def.setMinimumGeneralization(q, level);
                def.setMaximumGeneralization(q, level);
            }
        }
        for (String s : sensitive) {
            def.setAttributeType(s, AttributeType.SENSITIVE_ATTRIBUTE);
        }

        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new KAnonymity(p.k()));
        if (p.l() > 1) {
            for (String s : sensitive) {
                config.addPrivacyModel(new DistinctLDiversity(s, p.l()));
            }
        }
        config.setSuppressionLimit(p.suppressionLimit());

        ARXResult result = new ARXAnonymizer().anonymize(data, config);
        if (!result.isResultAvailable() || result.getGlobalOptimum() == null) {
            return Optional.empty();
        }
        ARXNode node = result.getGlobalOptimum();
        DataHandle out = result.getOutput();

        int[] col = new int[header.length];
        for (int c = 0; c < header.length; c++) {
            col[c] = out.getColumnIndexOf(header[c]);
        }
        int n = rows.size();
        List<String[]> outRows = new ArrayList<>(n);
        boolean[] suppressed = new boolean[n];
        long suppressedCount = 0;
        for (int i = 0; i < n; i++) {
            String[] r = new String[header.length];
            for (int c = 0; c < header.length; c++) {
                r[c] = out.getValue(i, col[c]);
            }
            outRows.add(r);
            suppressed[i] = out.isOutlier(i);
            if (suppressed[i]) {
                suppressedCount++;
            }
        }

        Map<String, Integer> levels = new LinkedHashMap<>();
        for (String q : qi) {
            levels.put(q, node.getGeneralization(q));
        }
        RiskModelSampleRisks risks = out.getRiskEstimator(ARXPopulationModel.create(ARXPopulationModel.Region.USA))
                .getSampleBasedReidentificationRisk();
        return Optional.of(new Outcome(outRows, suppressed, levels, risks.getHighestRisk(), risks.getAverageRisk(),
                suppressedCount, pinned != null));
    }
}
