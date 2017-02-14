#!/bin/bash

# to run this simply do ./run.sh <excercise letter>
# for example, ./run.sh A

case $1 in
    A)
        hadoop fs -rm -r /mp2/A-output
        hadoop jar TitleCount.jar TitleCount -Dstopwords=/mp2/misc/stopwords.txt \
            -Ddelimiters=/mp2/misc/delimiters.txt /mp2/titles /mp2/A-output
        hadoop fs -cat /mp2/A-output/part*
        ;;
    B)
        hadoop fs -rm -r /mp2/B-output
        hadoop jar TopTitles.jar TopTitles -Dstopwords=/mp2/misc/stopwords.txt \
            -Ddelimiters=/mp2/misc/delimiters.txt -DN=5 /mp2/titles /mp2/B-output
        hadoop fs -cat /mp2/B-output/part*
        ;;
    C)
        hadoop fs -rm -r /mp2/C-output
        hadoop jar TopTitleStatistics.jar TopTitleStatistics -Dstopwords=/mp2/misc/stopwords.txt \
            -Ddelimiters=/mp2/misc/delimiters.txt -DN=5 /mp2/titles /mp2/C-output
        hadoop fs -cat /mp2/C-output/part*
        ;;
    D)
        hadoop fs -rm -r /mp2/D-output
        hadoop jar OrphanPages.jar OrphanPages /mp2/links /mp2/D-output
        hadoop fs -cat /mp2/D-output/part*
        ;;
    E)
        hadoop fs -rm -r /mp2/E-output
        hadoop jar TopPopularLinks.jar TopPopularLinks -DN=5 /mp2/links /mp2/E-output
        hadoop fs -cat /mp2/E-output/part*
        ;;
    F)
        hadoop fs -rm -r /mp2/F-output
        hadoop jar PopularityLeague.jar PopularityLeague -Dleague=/mp2/misc/league.txt \
            /mp2/links /mp2/F-output
        hadoop fs -cat /mp2/F-output/part*
        ;;
    *)
        echo "Invalid exercise given"
        exit 1
esac