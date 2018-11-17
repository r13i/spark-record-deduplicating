# spark-record-deduplicating
Data cleansing problem statement: Data in a record are often duplicated. How do we find the duplicate probability ?


## Dataset
We'll be using the UC Irvine Machine Learning Repo dataset. From the book (see the References section) :
> The data set we’ll analyze was curated from a record linkage study per‐
> formed at a German hospital in 2010, and it contains several million pairs of patient
> records that were matched according to several different criteria, such as the patient’s
> name (first and last), address, and birthday. Each matching field was assigned a
> numerical score from 0.0 to 1.0 based on how similar the strings were, and the data
> was then hand-labeled to identify which pairs represented the same person and
> which did not. The underlying values of the fields that were used to create the data set
> were removed to protect the privacy of the patients. Numerical identifiers, the match
> scores for the fields, and the label for each pair (match versus nonmatch) were pub‐
> lished for use in record linkage research.

This data set is available at this URL : https://bit.ly/1Aoywaq

To download it, run this command : `$ curl -L -o data.zip https://bit.ly/1Aoywaq`


## How To

- `$ git clone https://github.com/redouane-dev/spark-record-deduplicating.git`
- `$ cd spark-record-deduplicating`
- `$ mkdir -p data/linkage`
- `$ curl -L -o data/data.zip https://bit.ly/1Aoywaq`
- `$ unzip -d ./data ./data/data.zip`
- `$ unzip -d ./data/linkage './data/block_*.zip`
- `$ rm -v ./data/block_*.zip`
- `$ ./gradlew build`
- `$ ./gradlew run`

## References
- "Advanced Analytics with Spark by Sandy Ryza, Uri Laserson, Sean Owen, and Josh Wills (O’Reilly). Copyright 2015 Sandy Ryza, Uri Laserson, Sean Owen, and Josh Wills, 978-1-491-91276-8."