# Analysis 1 - Monthly trend of plasma donations, antibody level (logIgG)
# and haemoglobin (Hb), per gender.


from pyspark import SparkContext

sc = SparkContext("local", "pyspark")
sc.setLogLevel("ERROR")
lines = sc.textFile("HA_Dataset_COVID_clean.csv")
header = lines.first()
data = lines.filter(lambda line: line != header)


fields = data.map(lambda line: line.split(","))


#key   = (year-month, gender)
#value = (1, logIgG, Hb)
pairs = fields.map(lambda f: ((f[13][:7], f[2]),
                              (1, float(f[11]), float(f[9]))))


sums = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))

# 6. sums -> means: (month & gender), (number of donations, mean logIgG, mean Hb)
means = sums.map(lambda kv: (kv[0], (kv[1][0],
                                     kv[1][1] / kv[1][0],
                                     kv[1][2] / kv[1][0])))


output = sorted(means.collect())          # sorted by motnth and gender
print("Total number of records:", data.count())
print("Number of (month, gender) groups:", means.count())


per_month = pairs.map(lambda kv: (kv[0][0], 1)).reduceByKey(lambda a, b: a + b)
busiest = per_month.collect()
busiest = max(busiest, key=lambda x: x[1])
print("Busiest month:", busiest[0], "with", busiest[1], "donations")
print()


print("Month    Gender  Donations  Mean logIgG  Mean Hb")
f = open("results_analysis1.txt", "w")
f.write("month,gender,donations,mean_logIgG,mean_Hb\n")
for (month, gender), (n, igg, hb) in output:
    print("%s  %s       %5i      %7.3f   %7.3f" % (month, gender, n, igg, hb))
    f.write("%s,%s,%i,%.3f,%.3f\n" % (month, gender, n, igg, hb))
f.close()

sc.stop()
