## NLDAS-3 Known Issues

This page tracks issues with the beta version of the forcing data.

If you discover any other problems with the data, please reach out
to David Mocko ([david.mocko@nasa.gov][1]), or submit a response to
the [user feedback form][2].

### Current Issues

**Spatial Distribution of Precip**

Although evaluations of precipitation against observations from
gauges show very good performance from NLDAS-3, we are investigating
ways to improve fine-scale details of the downscaled precipitation.
We have also noticed (and are working on correcting)
higher-than-expected precipitation values right along coastlines.

**Atmospheric Variable Discontinuities**

The fields of surface temperature/moisture/pressure and of LWdown at
the surface may show unrealistic discontinuities, especially just
inland of coastlines, due to the lapse-rate corrections.  We are
working to resolve these discontinuities.

**Negative Downward Shortwave**

Some grid points have negative SWdown values for a few hours during
the day in data from January 2019 and later.  These negative values
can just be considered to be zero.  However, we plan to set these
values to zero in the next version of the forcing.  Note that the
daily- and monthly-averaged forcing files currently do include these
negative values in the averages; the next version of these products
will also be corrected.

### Issues in Old Data Versions

**Lapse Rate Effects**

The previous set of "daily" files were produced with sub-optimal
lapse-rate values that were used for the lapse-rate correction of
Tair, Qair, PSurf, and the LWdown.  These files were produced in
May 2025, and should no longer be used.  The wind fields, SWdown,
and precipitation are the same as the previous data.

[1]:mailto:david.mocko@nasa.gov
[2]:https://docs.google.com/forms/d/e/1FAIpQLScL_LNT-YtKjQWiXqNOLak4JFxd5ETWvA3P7KzxK_WSE1swGg/viewform
