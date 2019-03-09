The problem description is given as follows:
A user may have multiple accounts and multiple phone numbers linked to that account.
A single account might be linked to one or more phone numbers.
A single phone number might be linked to one or more accounts.
No two users will have the same account.
No two users will have the same mobile number.

We have to find the number of unique users from this information.

A sample dataset may be given as follows:

| mb_num  | ac_num  |
+---------+---------+
| mob01   | acc02   |
| mob01   | acc01   |
| mob02   | acc02   |
| mob03   | acc01   |
| mob11   | acc11   |
| mob12   | acc11   |
| mob13   | acc11   |
| mob21   | acc21   |
| mob21   | acc22   |
| mob21   | acc23   | 

Expected Output for this dataset is 3.

Answer:

Calculate total unique records for mb_num
Ans: 7
Calculate total unique records for ac_num
Ans: 6
Total Unique records across the dataset = 13

Now, perform a double collect_set to create a set of two columns and calculate their array lengths. 
Note: Collect_set function will keep unique records only.

create collect_set table as
select mb_set, size(mb_set), collect_set(ac_num), size(collect_set(ac_num))
from
(select ac_num, collect_set(mb_num) mb_set from bank group by ac_num) z1
group by mb_set;

Calculate number of records of this table
Ans: 4

Calculate sum of size(mb_set) and size(collect_set(ac_num))
Ans: 14

Total Unique records are 13 but we are getting 14 elements. Since there is a difference of one and the array generated from the collect_set function cannot have duplicate values, we can safely assume that the collect_set table has two different rows having the same element. Since the two rows have the same element, they denote the same user. Subtract the excess row.
4 - (14-13) = 3

There are 3 unique users.
