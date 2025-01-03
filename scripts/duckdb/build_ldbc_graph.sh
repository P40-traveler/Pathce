#!/bin/bash
set -eu
set -o pipefail

# Build duckdb database from LDBC SNB dataset
# Argument: sf

sf=$1

workspace=$(realpath $(dirname $0)/../../)
duckdb=$workspace/tools/duckdb

mkdir -p $workspace/graphs/ldbc/duckdb/
cd $workspace/datasets/ldbc/sf$sf
rm -f "$workspace/graphs/ldbc/duckdb/ldbc_sf$sf.duckdb"
$duckdb "$workspace/graphs/ldbc/duckdb/ldbc_sf$sf.duckdb" <<EOF
begin;
create table City as from read_csv('City.csv');
create table Comment as from read_csv('Comment.csv');
create table Company as from read_csv('Company.csv');
create table Continent as from read_csv('Continent.csv');
create table Country as from read_csv('Country.csv');
create table Forum as from read_csv('Forum.csv');
create table Person as from read_csv('Person.csv');
create table Post as from read_csv('Post.csv');
create table Tag as from read_csv('Tag.csv');
create table TagClass as from read_csv('TagClass.csv');
create table University as from read_csv('University.csv');
create table City_isPartOf_Country as from read_csv('City_isPartOf_Country.csv');
create table Comment_hasCreator_Person as from read_csv('Comment_hasCreator_Person.csv');
create table Comment_hasTag_Tag as from read_csv('Comment_hasTag_Tag.csv');
create table Comment_isLocatedIn_Country as from read_csv('Comment_isLocatedIn_Country.csv');
create table Comment_replyOf_Comment as from read_csv('Comment_replyOf_Comment.csv');
create table Comment_replyOf_Post as from read_csv('Comment_replyOf_Post.csv');
create table Company_isLocatedIn_Country as from read_csv('Company_isLocatedIn_Country.csv');
create table Country_isPartOf_Continent as from read_csv('Country_isPartOf_Continent.csv');
create table Forum_containerOf_Post as from read_csv('Forum_containerOf_Post.csv');
create table Forum_hasMember_Person as from read_csv('Forum_hasMember_Person.csv');
create table Forum_hasModerator_Person as from read_csv('Forum_hasModerator_Person.csv');
create table Forum_hasTag_Tag as from read_csv('Forum_hasTag_Tag.csv');
create table Person_hasInterest_Tag as from read_csv('Person_hasInterest_Tag.csv');
create table Person_isLocatedIn_City as from read_csv('Person_isLocatedIn_City.csv');
create table Person_knows_Person as from read_csv('Person_knows_Person.csv');
create table Person_likes_Comment as from read_csv('Person_likes_Comment.csv');
create table Person_likes_Post as from read_csv('Person_likes_Post.csv');
create table Person_studyAt_University as from read_csv('Person_studyAt_University.csv');
create table Person_workAt_Company as from read_csv('Person_workAt_Company.csv');
create table Post_hasCreator_Person as from read_csv('Post_hasCreator_Person.csv');
create table Post_hasTag_Tag as from read_csv('Post_hasTag_Tag.csv');
create table Post_isLocatedIn_Country as from read_csv('Post_isLocatedIn_Country.csv');
create table TagClass_isSubclassOf_TagClass as from read_csv('TagClass_isSubclassOf_TagClass.csv');
create table Tag_hasType_TagClass as from read_csv('Tag_hasType_TagClass.csv');
create table University_isLocatedIn_City as from read_csv('University_isLocatedIn_City.csv');
commit;
EOF