# Analysis 3 - Relationship between number of donations and haemoglobin (Hb),
# per gender.


from pyspark import SparkContext
import math

sc = SparkContext("local", "pyspark")
sc.setLogLevel("ERROR")
lines = sc.textFile("HA_Dataset_COVID_clean.csv")
header = lines.first()
data = lines.filter(lambda line: line != header)


fields = data.map(lambda line: line.split(","))

# (donor, donation number, gender, age, Hb)
records = fields.map(lambda f: (f[0], int(f[1]), f[2], float(f[8]), float(f[9])))
records.cache()

# minimum Hb to donate in NL (mmol/L)
limit = {"F": 7.8, "M": 8.4}

print("Total number of records:", records.count())
print()


# Part A: Hb per donation number group and gender
def group(n):
    if n <= 3:
        return str(n)
    if n <= 5:
        return "4-5"
    if n <= 10:
        return "6-10"
    return "11+"

order = {"1": 0, "2": 1, "3": 2, "4-5": 3, "6-10": 4, "11+": 5}

#key   = (gender, group)
#value = (1, Hb, age, 1 if Hb below limit)
pairs = records.map(lambda r: ((r[2], group(r[1])),
                               (1, r[4], r[3], 1 if r[4] < limit[r[2]] else 0)))

sums = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3]))

means = sums.map(lambda kv: (kv[0], (kv[1][0],
                                     kv[1][1] / kv[1][0],
                                     kv[1][2] / kv[1][0],
                                     100.0 * kv[1][3] / kv[1][0])))

output = sorted(means.collect(), key=lambda kv: (kv[0][0], order[kv[0][1]]))

print("Part A - Hb per donation-number group and gender")
print("Gender  Donation nr  Donations  Mean Hb  Mean age  % below Hb limit")
f = open("results_analysis3_groups.txt", "w")
f.write("gender,donation_group,donations,mean_Hb,mean_age,pct_below_limit\n")
for (gender, grp), (n, hb, age, low) in output:
    print("%s       %-6s       %5i     %6.3f    %5.1f      %5.1f" % (gender, grp, n, hb, age, low))
    f.write("%s,%s,%i,%.3f,%.1f,%.1f\n" % (gender, grp, n, hb, age, low))
f.close()
print()


# Part B: correlation between donation number and Hb per gender
# value = (1, x, y, x*x, y*y, x*y)   x = donation number, y = Hb
pairs_b = records.map(lambda r: (r[2], (1, r[1], r[4], r[1] * r[1], r[4] * r[4], r[1] * r[4])))
sums_b = pairs_b.reduceByKey(lambda a, b: tuple(a[i] + b[i] for i in range(6)))

def pearson(s):
    n, sx, sy, sxx, syy, sxy = s
    return (n * sxy - sx * sy) / math.sqrt((n * sxx - sx * sx) * (n * syy - sy * sy))

corr = sorted(sums_b.mapValues(lambda s: (s[0], pearson(s))).collect())

print("Part B - Correlation between donation number and Hb")
print("Gender  Donations  Pearson r")
f = open("results_analysis3_correlation.txt", "w")
f.write("gender,donations,pearson_r\n")
for gender, (n, r) in corr:
    print("%s       %5i      %7.3f" % (gender, n, r))
    f.write("%s,%i,%.3f\n" % (gender, n, r))
f.close()
print()


# Part C: Hb at first vs last donation of the same donor
# key = donor, value = (gender, (nr, Hb) first, (nr, Hb) last)
per_donor = records.map(lambda r: (r[0], (r[2], (r[1], r[4]), (r[1], r[4]))))
first_last = per_donor.reduceByKey(lambda a, b: (a[0], min(a[1], b[1]), max(a[2], b[2])))

# only donors with 2 or more donations
repeat = first_last.filter(lambda kv: kv[1][2][0] > kv[1][1][0])

# value = (1, Hb change, Hb lower, extra donations, fell below limit)
changes = repeat.map(lambda kv: (kv[1][0],
                                 (1,
                                  kv[1][2][1] - kv[1][1][1],
                                  1 if kv[1][2][1] < kv[1][1][1] else 0,
                                  kv[1][2][0] - kv[1][1][0],
                                  1 if kv[1][1][1] >= limit[kv[1][0]] and kv[1][2][1] < limit[kv[1][0]] else 0)))

sums_c = changes.reduceByKey(lambda a, b: tuple(a[i] + b[i] for i in range(5)))

output_c = sorted(sums_c.mapValues(lambda s: (s[0], s[1] / s[0], 100.0 * s[2] / s[0],
                                              s[3] / s[0], 100.0 * s[4] / s[0])).collect())

print("Part C - Hb change between first and last donation (donors with >= 2 donations)")
print("Gender  Donors  Mean extra donations  Mean Hb change  % with lower Hb  % fell below limit")
f = open("results_analysis3_within_donor.txt", "w")
f.write("gender,donors,mean_extra_donations,mean_Hb_change,pct_lower_Hb,pct_fell_below_limit\n")
for gender, (n, d, low, extra, fell) in output_c:
    print("%s       %5i         %5.2f              %+6.3f          %5.1f            %5.1f"
          % (gender, n, extra, d, low, fell))
    f.write("%s,%i,%.2f,%.3f,%.1f,%.1f\n" % (gender, n, extra, d, low, fell))
f.close()

sc.stop()
