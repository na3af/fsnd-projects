# Log  Analysis
> Reporting Tool  : Building an informative summary from logs .

reporting tool that will use information from the database to discover what kind of articles the site's readers like.
The database contains newspaper articles, as well as the web server log for the site. 
The log has a database row for each time a reader loaded a web page. Using that information, I Build  reporting tool  to answer questions about the site's user activity.

# PreRequisites:
* [Python3](https://www.python.org/)
* [Vagrant](https://www.vagrantup.com/)
* [VirtualBox](https://www.virtualbox.org/)



# Setup Project:
1. Install Vagrant and VirtualBox
2. Download or Clone [fullstack-nanodegree-vm](https://github.com/udacity/fullstack-nanodegree-vm) repository.
3. Download the [data](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip) from here.
4. Unzip this file after downloading it. The file inside is called newsdata.sql.





# Usage:

#### Launching the Virtual Machine:
1. Launch the Vagrant VM inside Vagrant sub-directory in the downloaded fullstack-nanodegree-vm repository using command:
```
$ vagrant up
$ vagrant ssh
$ cd /vagrant  
```

#### Setting up the database :

```
$ psql -d news -f newsdata.sql
$ psql -d news
```
#### Setting up the Python :

```
$ pip install flask
$ pip install psycopg2
```
#### Running the app :

```
$ python3 app.py
```



