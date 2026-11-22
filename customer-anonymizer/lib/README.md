# ARX library

ARX is not published to Maven Central. Run:

```bash
./scripts/fetch-arx.sh
```

It downloads `libarx-3.9.1.jar` from https://github.com/arx-deidentifier/arx/releases, verifies its
SHA-256, and installs a trimmed copy into your local Maven repository as
`org.deidentifier.arx:libarx:3.9.1`. The Docker build runs the same script.

Why trimmed: the release jar is a fat jar that also bundles Hadoop, Guava, BouncyCastle and commons-*
copies that would shadow Spark's own once shaded into the job jar, while `libarx-3.9.1-min.jar` lacks
`de.linearbits.jhpl`, which is not on Maven Central. The script keeps only ARX's own packages
(`org.deidentifier`, `de.linearbits`); colt, hppc, exp4j and commons-validator are declared in
`anonymizer-profile-job/pom.xml`, and the rest come from Spark.

For CI, publish the trimmed jar to your internal artifact repository instead.
