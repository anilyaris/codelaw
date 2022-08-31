# Setting up data

Download the contents of this folder on your local system and follow the instructions.

**Prerequisites:** A running PostgreSQL server instance (```psql``` command can be successfully launched from the command line), the data provided by us already having been downloaded and extracted.

## Script execution order

Run the scripts in the order below with the corresponding arguments.

### ght-init

Initializes databases that will host GHTorrent data.

```bash
./ght-init.sh -h host -d datafolder
```

```host```: IP address of the PostgreSQL server or ```localhost``` if running locally

```datafolder```: PostgreSQL data folder (the directory specified when running ```initdb```)

### ght-download

Downloads and extracts GHTorrent data.

```bash
./ght-download.sh directory
```

```directory``` (optional): Directory to download the data, if not specified the data is downloaded and extracted into the current directory.

### ght-restore-pg

Loads GHTorrent data into PostgreSQL.

```bash
./ght-restore-pg.sh -h host dumpfolder
```

```host```: IP address of the PostgreSQL server or ```localhost``` if running locally

```dumpfolder``` (optional): Location of the GHTorrent CSV files, if not specified then the ```dump``` folder in the current directory is picked.

### newdata-init(-extended)

Sets up tables for our data. Run both scripts to set up all available tables.

```bash
./newdata-init(-extended).sh -h host
```

```host```: IP address of the PostgreSQL server or ```localhost``` if running locally

### newdata-restore(-extended)

Loads our data into PostgreSQL. Run both scripts to restore all available tables.

```bash
./newdata-restore(-extended).sh -h host datafolder
```

```host```: IP address of the PostgreSQL server or ```localhost``` if running locally

```datafolder```: Location of the CSV files after extracting our data
