# Minimum Reproducible Example of ST_Transform strangeness

The test-points script included should load sedona, start a spark session, register the sedona functions, create two points, and then do some operations on those points. 
It appears that there is some problem though with the ST_Point constructor, or with the ST_Transform function, as the sql below and table generated from it demonstrate.
The correct transformation is the one where we ST_FlipCoordinates, even though the Point is created with long/lat. 
The correctness of the coordinate transform is corroborated by https://epsg.io/transform#s_srs=4326&t_srs=5071&x=-117.1053969&y=33.1797200.

```sql
select
ST_Point(-117.105397, 33.17972) as p1,
ST_Transform(ST_Point(-117.105397, 33.17972), 'epsg:4326', 'epsg:5071', false) as p1t,
ST_Transform(ST_FlipCoordinates(ST_Point(-117.105397, 33.17972)), 'epsg:4326', 'epsg:5071', false) as p1_flip_t
```


| p1                           | p1t                                           | p1_flip_t                                    |
|------------------------------|-----------------------------------------------|----------------------------------------------|
| POINT (-117.105397 33.17972) | POINT (-8385712.402749769 -4228138.150187418) | POINT (-1939547.401032587 1339615.095082385) |


