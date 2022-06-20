# sift1m, k = 10
```
BF=105.3
a=1e-6, recall=0.283,  time=1.2
a=2e-6, recall=0.30,  time=1.4
a=5e-6, recall=0.34,  time=1.84
a=1e-5, recall=0.38,  time=2.08
a=5e-5, recall=0.514,  time=2.55
a=1e-4, recall=0.588,  time=2.98
a=2e-4, recall=0.703,  time=3.88
a=5e-4, recall=0.856,  time=5.82
a=1e-3, recall=0.938,  time=11.53
a=1.2e-3, recall=0.952,  time=12.63
a=1.5e-3, recall=0.961,  time=18.23
a=2e-3, recall=0.978,  time=21.93
a=3e-3, recall=0.989,  time=36.24
a=5e-3, recall=1,  time=92.06
```
# sift1m, k = 100
```
BF=105.3
a=1e-6, recall=0.10,  time=1.2
a=2e-6, recall=0.11,  time=1.4
a=5e-6, recall=0.13,  time=1.84
a=1e-5, recall=0.15,  time=2.08
a=5e-5, recall=0.21,  time=2.55
a=1e-4, recall=0.28,  time=2.98
a=2e-4, recall=0.37,  time=4.0
a=5e-4, recall=0.55,  time=6.0
a=1e-3, recall=0.713,  time=11.6
a=1.2e-3, recall=0.752,  time=12.8
a=1.5e-3, recall=0.788,  time=18.4
a=2e-3, recall=0.833,  time=24.7
a=3e-3, recall=0.891,  time=40.88
a=5e-3, recall=0.96,  time=92.5
a=1e-2, recall=0.98,  time=124.8
```

# gist1m, k = 10
```
BF=106.87
a=1e-6, recall=0.179,  time=1.4
a=2e-6, recall=0.187,  time=1.6
a=5e-6, recall=0.21,  time=1.84
a=1e-5, recall=0.239,  time=2.04
a=5e-5, recall=0.345,  time=2.68
a=1e-4, recall=0.45,  time=3.35
a=2e-4, recall=0.549,  time=4.21
a=5e-4, recall= 0.722,  time=6.9
a=1e-3, recall=0.837,  time=11.91
a=1.2e-3, recall=0.858,  time=14.14
a=1.5e-3, recall=0.882,  time=18.12
a=2e-3, recall=0.932,  time=23.3
a=3e-3, recall=0.964,  time=45.4
a=5e-3, recall=0.99,  time=92.06
```
# gist1m, k = 100
```
BF=105.3
a=1e-6, recall=0.054,  time=1.4
a=2e-6, recall=0.057,  time=1.6
a=5e-6, recall=0.065,  time=1.8
a=1e-5, recall=0.074,  time=2.04
a=5e-5, recall=0.134,  time=2.71
a=1e-4, recall=0.2,  time=3.5
a=2e-4, recall=0.26,  time=4.3
a=5e-4, recall=0.419,  time=7.12
a=1e-3, recall=0.567,  time=12.08
a=1.2e-3, recall=0.6,  time=14.2
a=1.5e-3, recall=0.65,  time=18.3
a=2e-3, recall=0.725,  time=23.5
a=3e-3, recall=0.817,  time=45.69
a=5e-3, recall=0.90,  time=90.5
a=1e-2, recall=0.97,  time=153.8
```
# n = 10 million , k = 10 

```
a=0.00001, recall=0.089, BFtime=1087, knn=2.9
a=0.00005, recall=0.268, knn=9.5
a=0.0001, recall=0.392, knn=18.5
a=0.0002, recall=0.486, knn=28.29
a=0.0005, recall=0.649, knn=64.5
a=0.0008, recall=0.783, knn=124.8
a=0.001, recall=0.795, knn=123.8
a=0.0012, recall=0.762, knn=123.8
a=0.0015, recall=0.877, knn=179.3
a=0.0018, recall=0.877, knn=179.3
a=0.002, recall=0.975, knn=337.5
a=0.005, recall=1, knn=575.0
a=0.006, recall=1, knn=575.0
```

# n = 10 million , k = 1
```
a=0.00001, recall=0.19, BFtime=1087, knn=2.9
a=0.00005, recall=0.44, knn=9.5
a=0.0001, recall=0.62, knn=18.5
a=0.0002, recall=0.77, knn=28.2
a=0.0005, recall=0.88, knn=64.5
a=0.0008, recall=0.89, knn=124.8
a=0.001, recall=0.95, knn=123.8
a=0.0012, recall=0.94, knn=123.8
a=0.0015, recall=1, knn=175.5
a=0.0018, recall=1, knn=179.3
a=0.002, recall=1, knn=329.5
a=0.005, recall=1, knn=563.0
```

# t-neighbor 10M_64
BF=574
r=0 gph=1.17 bktree=0.46
r=1 gph=1.63 bktree=4.63
r=2 gph=2.12 bktree=22.96
r=3 gph=6.24 bktree=76.7
r=4 gph=10.26 bktree=177.8
r=5 gph=13.9 bktree=331.5
r=6 gph=37.56 bktree=495.68
r=7 gph=60.157 bktree=692.768

# t-neighbor 1M_64
BF=55
r=0 gph=0.7 bktree=0.2
r=1 gph=0.8 bktree=1.1
r=2 gph=1 bktree=4.18
r=3 gph=1.7 bktree=11.4
r=4 gph=2.5 bktree=22.9
r=5 gph=3.3 bktree=36.5
r=6 gph=7.2 bktree=53.9
r=7 gph=11.2 bktree=68.4

