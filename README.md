# NLDAS-3

This repository contains documentation and instructions for
accessing preliminary data from the third phase of the
[North American Land Data Assimilation System][1] (NLDAS-3).

The project is currently in active development, working to improve
upon previous versions by:

- Improving the spatial resolution to 1-km.
- Expanding the spatial domain to all of North and Central America.
- Reducing forecast latency.
- Upgrading land surface model processes and parameterization.
- Assimilating a variety of remote sensing data in near real-time.

At this time, only the forcing data from 2001 to 2023 is publicly
available on AWS. The model outputs are in the final stage of
processing, and are expected to be released mid-2026.

Given that the data is in a beta stage of development, there are
several known issues [tracked in this document][8]. We are eager
to learn about any other problems encountered by our users, so please
let us know if you discover anything using the contact methods below.

## Data Description

The NLDAS-3 domain spans 7 to 72 degrees latitude, and -169 to -52
degrees longitude, which is represented on a 6,500 x 11,700 point
grid with 26,546,611 valid land points.

Ultimately, the dataset will support all the forcing, land surface
model, and routing model outputs
[tabulated on this page](info/variables.md).

Data are separated into hourly, daily, and monthly aggregated time
periods.

With the exception of precipitation, all of the data variables
represent the mean value within the UTC time period they represent.
Precipitation, however, provides the total accumulation over the
relevant UTC time period. Daily data also includes the minimum and
maximum values of each data variable for that day.

In order to support efficient access for multiple use cases, forcing
data are chunked into memory-adjacent blocks with shape
(6, 500, 900, 1) with respect to the (time, latitude, longitude,
variable) axes.

## Data Access

### AWS Bucket Contents

The forcing data is available in an AWS s3 bucket under
`s3://nasa-waterinsight/NLDAS3/forcing/`, with subdirectories
(keys) for multiple temporal resolutions as described below.

| resolution | file size | extension from `s3://nasa-waterinsight/NLDAS3/forcing/` |
| --- | --- | --- |
| **hourly** | ~12.8 GB | `hourly/{yyyymm}/NLDAS_FOR0010_H.A{yyyymmdd}.030.beta.nc` |
| **daily** | ~700 MB | `daily/{yyyymm}/NLDAS_FOR0010_D.A{yyyymmdd}.030.beta.nc` |
| **monthly** | ~575 MB | `monthly/{yyyy}/NLDAS_FOR0010_M.A{yyyymm}.030.beta.nc` |

Here, the curly-braced values are placeholders for fixed-width
integer dates.

In case you are a GrADS user, the AWS bucket endpoints also contain
.xdf template files so that you can use `xdfopen`.

Static parameter data is also available in the s3 bucket under
`s3://nasa-waterinsight/NLDAS3/static/lis_input.nldas3.noahmp401.1km.hymap.nc`,
which contains land mask, surface class, soil texture, surface
geometry, catchment ID, and other time-invariant parameters relevant
for land surface and routing model calculations.

### Access File Subset (s3fs)

Due to the large file sizes, it is often convenient to download a
subset of the data rather than the entire file. The most
widely-accepted way to do so is to open the file's bucket key using
[s3fs][12], which allows you to treat it like a file stored on your
local file system. This enables you to take advantage of the
memory-mapping ability of the HDF/netCDF format.

For a worked example using this approach, see [this notebook][11].

Subsetting files is also theoretically possible with only the netCDF4
library using http range requests as [described here][10], however
this method has not been thoroughly tested by our team.

### Download Full File (AWS CLI / boto)

If you just want to download an entire data file to your machine, the
most straightforward way to do so is by installing the [AWS CLI][9].

Once the CLI is installed, you can list the bucket content using a
command like the one below. Notice the trailing forward slash in the
bucket key; without it, the content of that subdirectory will not
be listed.

`aws s3 ls --human-readable s3://nasa-waterinsight/NLDAS3/forcing/daily/ --no-sign-request`

After you choose an available file, download it with the CLI as
follows:

`aws s3 cp s3://nasa-waterinsight/NLDAS3/forcing/daily/201007/NLDAS_FOR0010_H.A20100722.030.beta.nc . --no-sign-request`

## More Resources

- [Main project website](https://ldas.gsfc.nasa.gov/nldas/v3)
- [Earthdata Story on NLDAS-3](https://www.earthdata.nasa.gov/dashboard/stories/nldas)
- [Stakeholder workshop slides (4/10/2025)](https://ldas.gsfc.nasa.gov/sites/default/files/ldas/nldas/NLDAS-3_Drought-Workshop-3_Slides.pdf)
- [ARSET training on soil moisture for drought](https://appliedsciences.nasa.gov/get-involved/training/english/arset-application-nasa-sport-land-information-system-sport-lis-soil)

## Contact

For access to free Open Science Studio (JupyterHub) cloud resources
for testing the data, please fill out the
[NLDAS-3 Data Testing Request Form][2], and we will reach out to you
with more information.

We sincerely appreciate all user feedback; if you would like to share
your use cases and ideas for the future of this product, please fill
out the [user feedback form][3].

**Technical Contact**: David Mocko ([david.mocko@nasa.gov][4])

**Cloud Contact**: Mitchell Dodson ([mitchell.t.dodson@nasa.gov][5])

**Project PI**: Dr. Sujay Kumar ([sujay.v.kumar@nasa.gov][6])

**Project PI**: Dr. Chris Hain ([christopher.hain@nasa.gov][7])

[1]:https://ldas.gsfc.nasa.gov/nldas/v3
[2]:https://docs.google.com/forms/d/e/1FAIpQLScCCr5yxm0K8JsB8tnUwiRhxOCDXoeSry9hWqe0nYZYIzSv1g/viewform
[3]:https://docs.google.com/forms/d/e/1FAIpQLScL_LNT-YtKjQWiXqNOLak4JFxd5ETWvA3P7KzxK_WSE1swGg/viewform
[4]:mailto:david.mocko@nasa.gov
[5]:mailto:mitchell.t.dodson@nasa.gov
[6]:mailto:sujay.v.kumar@nasa.gov
[7]:mailto:christopher.hain@nasa.gov
[8]:info/known_issues.md
[9]:https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
[10]:https://docs.unidata.ucar.edu/netcdf-c/4.9.2/netcdf_byterange.html
[11]:user_data_notebooks/1-read_aws_data.ipynb
[12]:https://github.com/s3fs-fuse/s3fs-fuse
