# Joining 3 different sources into a 4th dataset

## Observations

### CSV format

It seems that the files are not following the standard CSV format.
I made an attempt at making a custom parser, but the time constraints and the abundance of corner cases didn't permit.
Such, I let pandas.read_csv to recover as much information as it could, skipping the bad lines.

### Choosing the final columns and the join column

In Analysis.xlsx you can visualize more easily the final column names and the contributing collumns from the joining CSVs.\
The matches are relatively easy to make, the column titles being very well picked. Here I have to clarify only one ambiguity: I consider that the "text" column from "google_dataset.csv" shouldn't be matched with "description" column from "facebook_dataset.csv". At first glance, intuition tricked me that they contain the same kind of information, but "text" column from "google_dataset.csv" seems to contain
reviews. I chose to retain both of them even if they are not of interest.

I identified 3 posible candidates for the joining column:

-   ❌ "name"/"legal_name" &rarr; &rarr; &rarr; &rarr; Not chosen due to being hard to pinpoint an exact match. The names were very simillar across tables, but small diferences lead to uncertainty and probability. Not desirable for a rigourous database.
-   ❌ "phone" &rarr; &rarr; &rarr; &rarr; &rarr; &rarr; &rarr; &rarr; &rarr; A pretty good candidate, especially because it tends to be unique(the majority of them are in international number format which is amazing), but a great number of rows in all datasets had this value missing.
-   ✅ "domain"/"root_domain" &rarr; &rarr; &rarr; By far the most abundant and the least prone to small changes out of all the possible collumns.

The rest of the collumns were more volatile, didn't withold enough distinct information or were simply missing from the other datasets.

### About the results

My primary objective was to deliver a rigorous dataset with consistency in mind.
Such, no full_joins/outer_joins were used as they may introduce data duplication. If one may desire to change this behaviour,
it can easily be done by changing the parameters of the pandas function in line 89 in main.py for main_v2.

I have implemented 2 versions for solving this problem.

-   The second one simply makes an inner join on "domain" using pandas. Found at main_v2. The result file is "main_v2_final.csv"
-   The first one, makes a join on "domain", takes the remaining rows and joins them on "phone"(the file: main_v1_rest_that_couldnt_be_joined_on_domain_so_it_was_joined_on_phone.csv). Then it tries to join the "domain" joined table, just to fill in the empty columns, not to add new rows as this may create untraceble duplicates. The final file is "main_v1_final.csv"

#### Choosing the column with the most relevant info

❗❗❗ Pandas merges datasets by concatenating columns. The merging logic of the columns is custom made and it is as follows:

1.  Get the list of candidates for the given cell
2.  Remove any NaN value from the list of candidates.
3.  Create a tournament with the help of the library fuzzywuzzy(Levenshtein Distance). Every candidate votes a score of similarity for every candidate(including self). The candidate with the most points wins.

Regarding the fuzzywuzzy tournament, I chose to stringify, stripe and lowercase the strings to enhance matching. I also used a matcher that
doesn't take into account the order of the words.

❗❗❗The tiebreaker for the fuzzywuzzy tournament❗❗❗ is the precedence. The first element in the list will be the one taken. Such, it makes prioritization of the datasets easier by simply changing their order. My personal choice is Facebook &rarr; Google &rarr; Website , due to Facebook having the most relevant data, followed by Google which had the most data and followed by Website which being anonymous didn't inspire too much credibility.

## How to run

Install dependencies with pip3.

    pip install -r requirements.txt

In directory ./data/in unzip provided datasets.zip . The files "facebook_dataset.csv", "google_dataset.csv" and "website_dataset.csv" must be available in this directory.
Simply run python3 main.py . Don't forget to take a look in main.py to see both v1 and v2.
