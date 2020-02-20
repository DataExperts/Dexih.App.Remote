
# Data Experts Integration Hub - Remote Agent

The remote agent allows data integration jobs to be built and executed through the integration hub.

The remote agent operates in conjunction with the data experts integration hub (DexIH).  

To get started register a new account at [dexih.dataexpertsgroup.com][dexih]

[![][dex-img]][dex]

[dex-img]: http://dataexpertsgroup.com/assets/img/dex_web_logo.png
[dex]: https://dataexpertsgroup.com
[dexih]: https://dexih.dataexpertsgroup.com

## Features

* Connectivity for the following data sources:
    * Microsoft Sql Server
    * PostgresSql
    * MySql
    * Sqlite
    * MongoDB
    * Oracle
    * DB2
    * Data files in either delimited, Json or Xml formats
    * Data files located either locally, ftp, sftp, or azure storage
    * Rest based web services
    * Azure storage tables
    * Excel files

* Build in data transformation capabilities:
    * Column mappings (with over 200 built in functions)
    * Custom functions, using c#
    * Grouping and Aggregation (e.g. sum, average etc)
    * Statistical functions (e.g. variance, moving average etc.)
    * Machine Learning functions (using the ML.Net Library)
    * Row Pivoting
    * Filtering
    * Sorting
    * Joining
    * Lookups
    * Column validation and automated row cleaning or rejection
    * Data profiling

* Jobs Scheduling:
    * Jobs can specify complex sequences of datalinks.
    * Recurring schedules can be specified.
    * Scheduling available to the second.

* Security / Privacy:
    * Column encryption and decryption
    * Column hashing
    * Configurable privacy settings can ensure data is kept internally

* Data sharing:
    * Tables and data can be shared and downloaded easily be published internally or externally
    * Datalinks can be connected to data shared on other remote agents as a data source


## Installation

The remote agent should be run in a location that has access to the required databases.  It can be run on Windows, Mac OSX, and many Linux systems (a full list of supported systems is [here](https://github.com/dotnet/core/blob/master/release-notes/2.0/2.0-supported-os.md)).

### Automated Installation

1. Log into the data experts integration hub, navigate to a Hub, select **Remote Agents**, and **Download Application**.
2. Complete the form, and use the help (? at top of page) for installation directions.

### Manual Installation

1. Download the latest remote agent release [here](https://github.com/DataExperts/Dexih.App.Remote/releases/latest).
2. Unzip the files into the required directory.
3. Open a command prompt and execute the command `./dexih.remote`.
4. Follow the prompts to complete the installation.

### Ubuntu Installation

Run the following command to automatically download the agent and get started:
```
curl -o dexih.remote.run.sh https://raw.githubusercontent.com/DataExperts/Dexih.App.Remote/master/src/dexih.remote/scripts/dexih.remote.run.linux.sh
chmod a+x dexih.remote.run.sh
./dexih.remote.run.sh
```

### Configuration Options

The `dexih.remote` has a number of configuration options.  When executed these options are set in the following order:

* Using the appsettings.json file.
* Using environment variables.
* Using command line options.
* Using prompted information.

The `appsettings.json` which is contained in the same directory as the 


### 

## Credits

See the [dexih.transforms](https://github.com/DataExperts/dexih.transforms) project for additional credits.

## License

MIT © [Data Experts Group](mailto:gholland@dataexpertsgroup.com)
