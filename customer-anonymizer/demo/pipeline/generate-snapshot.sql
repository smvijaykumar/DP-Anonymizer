-- Deterministic synthetic customer snapshot (no real data). One partition so seeded rand() is stable.
-- Columns follow config/common-schema.conf; the profile job reads only id, quasi-identifiers and
-- sensitive columns, the rest are here so the source looks like a real customer table.
INSERT OVERWRITE DIRECTORY '${env:SNAPSHOT_PATH}' USING parquet
SELECT
  customerId,
  firstName,
  lastName,
  lower(firstName) || '.' || lower(lastName) || substr(customerId, 4) || '@example.com' AS email,
  '555' || lpad(CAST(id AS STRING), 7, '0')                                            AS phone,
  dob,
  gender,
  zip,
  occupation,
  incomeBand
FROM (
  SELECT
    id,
    format_string('C%06d', id) AS customerId,
    element_at(array('Ana','Ben','Chloe','Dev','Elena','Farid','Grace','Hiro','Ines','Jon','Kemi','Liam',
                     'Maya','Noah','Olga','Priya','Quinn','Rosa','Sam','Tara','Umar','Vera','Wei','Yara'),
               CAST(floor(rand(101) * 24) AS INT) + 1) AS firstName,
    element_at(array('Diaz','Lee','Okafor','Patel','Novak','Smith','Haddad','Tanaka','Silva','Brown',
                     'Kowalski','Nguyen','Garcia','Muller','Rossi','Cohen','Ivanova','Khan'),
               CAST(floor(rand(102) * 18) AS INT) + 1) AS lastName,
    date_add(DATE'1950-01-01', CAST(floor(rand(103) * 18250) AS INT)) AS dob,
    IF(rand(104) < 0.5, 'F', 'M') AS gender,
    element_at(array('33326','33327','33331','33332','10001','10002','10003','10011',
                     '94105','94107','94110','60601','60605','60614','73301','73344'),
               CAST(floor(rand(105) * 16) AS INT) + 1) AS zip,
    element_at(array('nurse','physician','pharmacist','teacher','professor','software engineer',
                     'engineer','accountant','analyst','driver','pilot','chef','retail associate',
                     'artist','farmer'),
               CAST(floor(rand(106) * 15) AS INT) + 1) AS occupation,
    element_at(array('LOW','MID','HIGH','VERY_HIGH'), CAST(floor(rand(107) * 4) AS INT) + 1) AS incomeBand
  FROM range(0, ${env:CUSTOMERS}, 1, 1)
);
