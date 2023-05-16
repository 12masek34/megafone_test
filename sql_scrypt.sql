
-- Структура данных.

CREATE TABLE IF NOT EXISTS servers (
    srv_id NUMBER PRIMARY KEY,
    srv_name VARCHAR2(50)
);

CREATE TABLE IF NOT EXISTS server_hdd (
    hdd_id NUMBER PRIMARY KEY,
    srv_id NUMBER REFERENCES servers(srv_id),
    hdd_name VARCHAR2(50),
    hdd_capacity NUMBER
  );

CREATE TABLE IF NOT EXISTS hdd_monitoring (
    hdd_id NUMBER REFERENCES server_hdd(
    used_space NUMBER,
    formatted_space NUMBER,
    monitoring_date DATE
  );


-- a.
-- Вывести серверы, суммарная емкость накопителей которых больше 110 ТБ и
-- менее 130 ТБ. Без использования подзапросов.

SELECT s.srv_name, sum(h.hdd_capacity) AS total_capacity
FROM servers s
JOIN server_hdd h ON s.srv_id = h.srv_id
GROUP BY s.srv_name
HAVING SUM(h.hdd_capacity) > 110 AND SUM(h.hdd_capacity) < 130


-- b.
-- Вследствие ошибки в таблице server_hdd появились дубли строк.
-- Предложите вариант удаления дубликатов, оставив только уникальные строки.

DELETE FROM server_hdd
WHERE hdd_id NOT IN (
  SELECT MIN(hdd_id)
  FROM server_hdd
  GROUP BY srv_id, hdd_name, hdd_capacity
);

-- c.
-- Какими средствами СУБД Oracle Вы в дальнейшем предотвратили бы появления
-- дубликатов строк?

-- Добавить ограничение уникатьности всех полей.
ALTER TABLE server_hdd ADD CONSTRAINT uniq_server_hdd UNIQUE (hdd_capacity, hdd_name, srv_id);


-- d.
-- Вывести изменение занятой емкости на самых больших дисках каждого сервера в
-- формате:
-- Имя сервера, Имя диска, Общая емкость диска, Предыдущая занятая емкость, Текущая
-- занятая емкость диска, Дата мониторинга.

SELECT
  s.srv_name,
  c.hdd_name,
  c.hdd_capacity,
  c.prev_used_space,
  c.used_space,
  c.monitoring_date
FROM (
  SELECT se.srv_id, se.srv_name, max(hd.hdd_capacity) AS max_c
    FROM servers se
    JOIN server_hdd hd ON se.srv_id = hd.srv_id
    GROUP BY se.srv_id) AS s
    JOIN server_hdd h ON s.srv_id = h.srv_id and s.max_c = h.hdd_capacity
    JOIN (
      SELECT
        h.hdd_id,
        sh.hdd_name,
        sh.hdd_capacity,
        LAG(h.used_space, 1) OVER (PARTITION BY h.hdd_id ORDER BY h.monitoring_date) AS prev_used_space,
        h.used_space,
        h.monitoring_date,
        ROW_NUMBER() OVER (PARTITION BY sh.srv_id, h.hdd_id ORDER BY h.monitoring_date DESC) AS rn
      FROM hdd_monitoring h
      JOIN server_hdd sh ON sh.hdd_id = h.hdd_id
      JOIN (
        SELECT hdd_id, MAX(monitoring_date) AS last_date
        FROM hdd_monitoring
        GROUP BY hdd_id
      ) l ON h.hdd_id = l.hdd_id AND h.monitoring_date = l.last_date
    ) c ON h.hdd_id = c.hdd_id AND c.rn <= 10
    ORDER BY s.srv_name, c.hdd_capacity DESC;
