# What is the Dexih Remote Agent?

The **D**ata **Ex**perts **I**nformation **H**ub (Dexih) is a cloud based information management platform, which allows for data integration, sharing and analysis.  Support is included for all common database platforms, file formats, and operating systems.  Features include ETL processing, advanced analytics and statistics and machine learning.

The Dexih Remote is a local agent which connects with data sources, runs data process and manages batch schedules.  Once configured, the remote agent will connect automatically with the central Integration Hub, which is the front end portal for managing data.

To get started, create a (free) account is required by registering at the [Integration Hub](https://dexih.dataexpertsgroup.com).    

![logo](http://dataexpertsgroup.com/img/dex_web_logo.png)

# How to use this image

The following command will run a (first time) interactive session which will include prompts to configure the remote agent to connect with the central integration hub.

```console
$ docker run -it  --name dexih-remote -p 33944:33944 dexih/remote
```

The following environment variables are also available when running the instance:

-	`-e DEXIH_CONFIG_DIRECTORY=/path to configuration information`
-	`-e DEXIH_LOG_DIRECTORY=/path to log files`

To run a non-interactive instance, a configuration file must be first downloaded from the integration hub as follows:

- Log into the integration hub, select configure --> Remote Agents --> Download Agent.
- Complete the configuration form an select "Download Settings".
- Copy the downloaded settings file, into a 'config' directory.

Then run the instance:
```console
$ docker run --name dexih-remote \
   -p 33944:33944 \
   -e DEXIH_CONFIG_DIRECTORY=/config \
   -e Logging__LogFilePath=/log \
   -e Network__LocalIpAddress=192.168.1.2
   -v /local_config_path:/config \
   -v /local_log_files_path:/config \
   -v /data_files_path:/data \
   dexih/remote
```

The "Network__LocalIpAddress" is provides the network that docker host is on, meaning if a port mapping has been set (i.e. -p 33944:33944) the integration hub will allow data upload/download to happen through the local network.

Other environment variables can be set, by referring to the setting in the appsettings.json file (in the config directory).  These settings will map to environment variables using the section and double underscore.  For example to set a default user
```console
   -e AppSettings__User=user@domain.com
```

# License

View [license information](https://dexih.dataexpertsgroup.com/auth/terms) for the software contained in this image.

As for any pre-built image usage, it is the image user's responsibility to ensure that any use of this image complies with any relevant licenses for all software contained within.
