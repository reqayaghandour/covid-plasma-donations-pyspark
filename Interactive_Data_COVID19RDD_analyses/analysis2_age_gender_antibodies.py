# Analysis 2 - Antibody level (logIgG and concentration) per age group and gender.


from pyspark import SparkContext

sc = SparkContext("local", "pyspark")
sc.setLogLevel("ERROR")
lines = sc.textFile("HA_Dataset_COVID_clean.csv")
header = lines.first()
data = lines.filter(lambda line: line != header)


fields = data.map(lambda line: line.split(","))

# (gender, age, logIgG, Conc)
records = fields.map(lambda f: (f[2], float(f[8]), float(f[11]), float(f[12])))
records.cache()

n_total = records.count()
print("Total number of records:", n_total)


# threshold for "high antibody": top 25% of all concentrations
sorted_conc = records.map(lambda r: r[3]).sortBy(lambda x: x)
idx = int(0.75 * n_total)
high = sorted_conc.zipWithIndex().filter(lambda x: x[1] == idx).map(lambda x: x[0]).first()
print("High antibody threshold (75th percentile of Conc): %.2f" % high)
print()


def age_group(age):
    if age < 30:
        return "18-29"
    if age < 40:
        return "30-39"
    if age < 50:
        return "40-49"
    if age < 60:
        return "50-59"
    return "60+"


#key   = (age group, gender)
#value = (1, logIgG, Conc, 1 if high antibody)
pairs = records.map(lambda r: ((age_group(r[1]), r[0]),
                               (1, r[2], r[3], 1 if r[3] >= high else 0)))

sums = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3]))

# sums -> (donations, mean logIgG, mean Conc, % high antibody)
means = sums.map(lambda kv: (kv[0], (kv[1][0],
                                     kv[1][1] / kv[1][0],
                                     kv[1][2] / kv[1][0],
                                     100.0 * kv[1][3] / kv[1][0])))

output = sorted(means.collect())


# group with the highest share of high-antibody donations
best = means.max(key=lambda kv: kv[1][3])
print("Group with most high-antibody donations:", best[0][0], best[0][1],
      "(%.1f%%)" % best[1][3])
print()


print("Age group  Gender  Donations  Mean logIgG  Mean Conc  % high antibody")
f = open("results_analysis2.txt", "w")
f.write("age_group,gender,donations,mean_logIgG,mean_Conc,pct_high_antibody\n")
for (grp, gender), (n, igg, conc, pct) in output:
    print("%-9s  %s       %5i      %7.3f    %7.2f        %5.1f" % (grp, gender, n, igg, conc, pct))
    f.write("%s,%s,%i,%.3f,%.2f,%.1f\n" % (grp, gender, n, igg, conc, pct))
f.close()

sc.stop()
