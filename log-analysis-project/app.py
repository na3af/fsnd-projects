import psycopg2

conn = psycopg2.connect("dbname=news")
cursor = conn.cursor()
cursor.execute(
    "SELECT Substring (path, 10) AS Article_Name, "
    "       Count(*)             AS number_Of_View "
    "FROM   log "
    "WHERE  status = '200 OK' "
    "    AND path != '/' "
    "GROUP  BY path "
    "ORDER  BY Count(*) DESC "
    "LIMIT  3; "
)
results = cursor.fetchall()

print('"' + results[0][0] + '"' + " - " + str(results[0][1]) + " Views .")
print('"' + results[1][0] + '"' + " - " + str(results[1][1]) + " Views .")
print('"' + results[2][0] + '"' + " - " + str(results[2][1]) + " Views .")


cursor.execute(
    "SELECT authors.NAME,"
    "   Count(*) "
    "FROM   log "
    "    JOIN articles "
    "        ON Substring (log.path, 10) = articles.slug "
    "    JOIN authors "
    "       ON authors.id = articles.author "
    "WHERE  path != '/' "
    "    AND status = '200 OK' "
    "GROUP  BY authors.id "
    "ORDER  BY Count(*) DESC; "
)

results = cursor.fetchall()
for i in range(0, len(results)):
    print(results[i][0] + " - " + str(results[i][1]) + " Views .")

cursor.execute(
    "SELECT TO_CHAR(DATE(log.time), 'Mon DD, YYYY'), "
    "   Round(100.0 * Sum(CASE "
    "                       WHEN log.status = '200 OK' THEN 0 "
    "                       ELSE 1 "
    "                     END) / Count(*), 2) AS percentage_of_errors "
    "FROM   log "
    "GROUP  BY Date (log.time) "
    "HAVING Round(100.0 * Sum(CASE "
    "                       WHEN log.status = '200 OK' THEN 0 "
    "                       ELSE 1 "
    "                    END) / Count(*), 2) > 1; "
)
results = cursor.fetchall()

print(
    str(results[0][0]) + " - "
    "" + str(results[0][1]) + "%  errors .")


conn.close()
