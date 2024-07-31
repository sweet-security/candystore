# Vicky Simulator

* Tests the fill level that each shard can have using different params
* Tests the number of collisions in the same row (signatures)
* Tests the performance of position_simd for various sizes (compile with `--release`)

# Results
```
r=  32 w=  32 avg=0.687102 elems=   1024 sz=  12KB collisions=0 collisions-probability=0.000000115483993  
r=  32 w=  64 avg=0.755089 elems=   2048 sz=  24KB collisions=0 collisions-probability=0.000000469386467  
r=  32 w= 128 avg=0.832785 elems=   4096 sz=  48KB collisions=0 collisions-probability=0.000001892445681 GOOD 
r=  32 w= 256 avg=0.871744 elems=   8192 sz=  96KB collisions=0 collisions-probability=0.000007599563332 GOOD 
r=  32 w= 512 avg=0.907163 elems=  16384 sz= 192KB collisions=0 collisions-probability=0.000030457509641 GOOD 
r=  32 w=1024 avg=0.935280 elems=  32768 sz= 384KB collisions=0 collisions-probability=0.000121943667477 GOOD 
r=  64 w=  32 avg=0.647315 elems=   2048 sz=  24KB collisions=0 collisions-probability=0.000000115483993  
r=  64 w=  64 avg=0.728652 elems=   4096 sz=  48KB collisions=0 collisions-probability=0.000000469386467  
r=  64 w= 128 avg=0.805568 elems=   8192 sz=  96KB collisions=0 collisions-probability=0.000001892445681 GOOD 
r=  64 w= 256 avg=0.853133 elems=  16384 sz= 192KB collisions=0 collisions-probability=0.000007599563332 GOOD 
r=  64 w= 512 avg=0.899420 elems=  32768 sz= 384KB collisions=0 collisions-probability=0.000030457509641 GOOD 
r=  64 w=1024 avg=0.927043 elems=  65536 sz= 768KB collisions=6 collisions-probability=0.000121943667477 GOOD 
r= 128 w=  32 avg=0.615332 elems=   4096 sz=  48KB collisions=0 collisions-probability=0.000000115483993  
r= 128 w=  64 avg=0.708627 elems=   8192 sz=  96KB collisions=0 collisions-probability=0.000000469386467  
r= 128 w= 128 avg=0.784355 elems=  16384 sz= 192KB collisions=0 collisions-probability=0.000001892445681  
r= 128 w= 256 avg=0.843362 elems=  32768 sz= 384KB collisions=0 collisions-probability=0.000007599563332 GOOD 
r= 128 w= 512 avg=0.884743 elems=  65536 sz= 768KB collisions=0 collisions-probability=0.000030457509641 GOOD 
r= 128 w=1024 avg=0.920297 elems= 131072 sz=1536KB collisions=3 collisions-probability=0.000121943667477 GOOD BIG
r= 256 w=  32 avg=0.599061 elems=   8192 sz=  96KB collisions=0 collisions-probability=0.000000115483993  
r= 256 w=  64 avg=0.688738 elems=  16384 sz= 192KB collisions=0 collisions-probability=0.000000469386467  
r= 256 w= 128 avg=0.768617 elems=  32768 sz= 384KB collisions=0 collisions-probability=0.000001892445681  
r= 256 w= 256 avg=0.832496 elems=  65536 sz= 768KB collisions=0 collisions-probability=0.000007599563332 GOOD 
r= 256 w= 512 avg=0.877548 elems= 131072 sz=1536KB collisions=0 collisions-probability=0.000030457509641 GOOD BIG
r= 256 w=1024 avg=0.914863 elems= 262144 sz=3072KB collisions=6 collisions-probability=0.000121943667477 GOOD BIG
```

```
width=  32 time per simd=   4ns
width=  64 time per simd=  21ns
width= 128 time per simd=  26ns
width= 256 time per simd=  36ns
width= 512 time per simd=  59ns
width=1024 time per simd= 100ns
```

```
width=  32 time per non-simd=  25ns
width=  64 time per non-simd=  53ns
width= 128 time per non-simd=  85ns
width= 256 time per non-simd= 145ns
width= 512 time per non-simd= 266ns
width=1024 time per non-simd= 507ns
```
