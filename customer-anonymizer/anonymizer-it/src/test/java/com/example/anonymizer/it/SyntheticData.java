package com.example.anonymizer.it;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Deterministic synthetic customers. No real data is used anywhere in tests. */
final class SyntheticData {
    static final String[] ZIPS = {"33326", "33327", "33331", "10001", "10002", "10003", "94105", "94107"};
    static final String[] OCCUPATIONS = {"nurse", "teacher", "engineer", "accountant", "driver", "chef"};
    static final String[] INCOME = {"LOW", "MID", "HIGH", "VERY_HIGH"};

    record Customer(String id, String firstName, String lastName, String email, String phone, LocalDate dob,
                    String gender, String zip, String occupation, String incomeBand) {
    }

    private SyntheticData() {
    }

    static List<Customer> customers(int n, long seed) {
        Random r = new Random(seed);
        List<Customer> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(new Customer(
                    String.format("C%05d", i),
                    String.format("First%05d", i),
                    String.format("Last%05d", i),
                    String.format("user%05d@example.com", i),
                    String.format("555%07d", i),
                    LocalDate.of(1950 + r.nextInt(50), 1 + r.nextInt(12), 1 + r.nextInt(28)),
                    r.nextBoolean() ? "M" : "F",
                    ZIPS[r.nextInt(ZIPS.length)],
                    OCCUPATIONS[r.nextInt(OCCUPATIONS.length)],
                    INCOME[r.nextInt(INCOME.length)]));
        }
        return out;
    }

    static String eventJson(Customer c, String eventType, String eventTs) {
        return "{\"customerId\":\"" + c.id() + "\",\"firstName\":\"" + c.firstName() + "\",\"lastName\":\"" + c.lastName()
                + "\",\"email\":\"" + c.email() + "\",\"phone\":\"" + c.phone() + "\",\"dob\":\"" + c.dob()
                + "\",\"gender\":\"" + c.gender() + "\",\"zip\":\"" + c.zip() + "\",\"occupation\":\"" + c.occupation()
                + "\",\"incomeBand\":\"" + c.incomeBand() + "\",\"eventType\":\"" + eventType + "\",\"eventTs\":\""
                + eventTs + "\"}";
    }
}
