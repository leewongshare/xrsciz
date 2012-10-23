"""
Conditional probability of words over time.

    Pr( word | year )

Calculated with Google Ngram Dataset hosted on Amazon S3.

$ python word-choice.py -r emr \
-o s3://selik.org.emr/word-prob-x \
--no-output \
s3://selik.org.emr/ngrams/1grams/googlebooks-eng-all-1gram-20120701-x.gz

I uploaded the a, q, and x files to s3://selik.org.emr/ngrams/1grams/
Please use the default (us-east-1) Amazon cloud location. Otherwise we might incur data transfer charges.

For example

http://storage.googleapis.com/books/ngrams/books/datasetsv2.html
http://aws.amazon.com/datasets/8172056142375670
http://aws.amazon.com/articles/5249664154115844

The 2009 data version includes pages count, the 2012 data version does not.

Version 1, 2009
ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
year TAB match_count TAB page_count TAB volume_count NEWLINE

Version 2, 2012
ngram TAB year TAB match_count TAB volume_count NEWLINE
year TAB match_count TAB volume_count NEWLINE

Note: Elastic MapReduce uses older versions of Python by default.
"""

from mrjob.job import MRJob
import re

real_word = re.compile(r"^[A-Za-z+'-]+$")

class WordProb(MRJob):

    def map_raw_by_year(self, _, line):
        tokens = line.split()
        try:
            ngram = tokens[0]
            if real_word.match(ngram):
                year = int(tokens[1])
                count = int(tokens[2])
                yield (year, (ngram, count))
        except:
            pass

    def top_100_by_year(self, year, counts):
        keyfunc = lambda ngram_count: ngram_count[1]
        counts = sorted(counts, key = keyfunc, reverse = True)
        total = sum([ngram_count[1] for ngram_count in counts])
        for ngram, count in counts[:100]:
            yield (year, (ngram, count, total))

    def steps(self):
        return ([self.mr(self.map_raw_by_year,
                         self.top_100_by_year)])

if __name__ == '__main__':
    WordProb.run()
