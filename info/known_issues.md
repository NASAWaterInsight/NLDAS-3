## NLDAS-3 Known Issues

This page tracks issues with the beta version of the forcing data.

If you discover any other problems with the data, please reach out
to David Mocko ([david.mocko@nasa.gov][1]), or submit a response to
the [user feedback form][2].

### Current Issues

**Spatial Distribution of Precipitation**

Although evaluations of precipitation against observations from
gauges show very good performance from NLDAS-3, we are investigating
ways to improve fine-scale details of the downscaled precipitation.
The "beta" version of the forcing has higher-than-expected
precipitation values right along coastlines. The final version of
the forcing (which will be available soon) corrects this issue.

**Atmospheric Variable Discontinuities**

The "beta" version of the forcing shows unrealistic discontinuities
in the fields of surface temperature/moisture/pressure and of LWdown
at the surface especially just inland of coastlines, due to the
lapse-rate corrections. The final version of the forcing corrects
this issue.

**Negative Downward Shortwave**

The "beta" version of the forcing has a few grid points for a few
hours during the day with negative SWdown values (especially January
2019 and later). The "beta" daily- and monthly-averaged forcing files
include these negative values in the averages. The final version of
the forcing corrects this issue.

### Issues in Old Data Versions

**Lapse Rate Effects**

The previous set of "daily" files were produced with sub-optimal
lapse-rate values that were used for the lapse-rate correction of
Tair, Qair, PSurf, and the LWdown.  These files were produced in
May 2025, and should no longer be used.  The wind fields, SWdown,
and precipitation are the same as the previous data.

[1]:mailto:david.mocko@nasa.gov
[2]:https://docs.google.com/forms/d/e/1FAIpQLScL_LNT-YtKjQWiXqNOLak4JFxd5ETWvA3P7KzxK_WSE1swGg/viewform
