+============================================================================+
DRUPAL FRANKENSCRAPER
+============================================================================+

Originally developed against the Social Media Today drupal site, this script
uses a combination of direct database querying and HTTP calls to gather
content from the drupal site as well as data about the content and the users
that submitted it.

Usage
-------
1) Create and start a Python2.7 virtual environment
2) Install required packes with pip:
	$ pip install -r requirements.txt
3) Copy settings.py.example to settings.py and change it as needed for your
   environment (NOTE: it is initially set up to run against an SSH tunnel to
   a platform.sh hosted project)
4) Run the script:
	$ python frankscraper.py

   If you're just testing, probably add a limit:
	$ python frankscraper.py --limit=10

   You can do just a dry run to get information about what the script would
   do, including the full node query:
	$ python frankscraper.py --dry-run

   Specify the epoch value of the `changed` field you wish to export from:
	$ python frankscraper.py --epoch-changed=1416503465
	(this will query for stories with a changed value greater than
	1416503465)

	If not specified, the script will look for a value in a file called
	.highest_changed_epoch, which would have been set by a previous run
	of the script. This is updated each time the script successfully writes
	the data for a story to the corresponding output file, so if the script
	fails, it is likely you can have it pick up where it left off.

	If the .highest_changed_epoch file is not found, the script will use 0.

Output
-------
Each time you run the frankenscraper, it will create a new directory in the
output folder, named with the current date and time. Inside that folder, you
should get 3 files:
	1) frankenscraper.log
		All logged messages (only INFO will print to the screen,
		but INFO and DEBUG statements print to the log)
	2) story.jl
		A JSON lines file, where each line is a JSON formatted string with
		the content and metadata of an SMT story
	3) user.jl
		A JSON lines file, where each line is a JSON formatted string with the
		profile page content and metadata of an SMT story. For every "uid" in
		story.jl, there should be a row in user.jl