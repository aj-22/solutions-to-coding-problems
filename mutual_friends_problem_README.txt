Pipeline Sketch:
    1. Import the data and map into a key value pairs (KV pairs) of (Person,List[Person's Friends]])
    K=Person, V=List
    2. Map the imported data into dictionary for quick lookup in next step
    3. Function which explodes (Person,List[Person's Friends]]) into key-value pairs of  of
        (Other Person, (Reccomended Friend, Mutual Friend))
            where Other Person and Reccommended Friend are friend's from List
            and Mutual Friend is the Person
    For example:
        Input: (1, [4,5,6]) #Person = 1, List = [4,5,6]
        Output: (4,(5,1), (4,(6,1)), (5,(4,1)), ...
    Exclusions: Reccomended Friend who is already a friend of Other Person, If Reccomended Friend = Other Person
    4. Reduce (Other Person, (Reccomended Friend, Mutual Friend)) into
        (Other Person, [(Reccommended Friend, Number of Mutual Friends)]
    5. Reduce (Other Person, [(Reccommended Friend, Number of Mutual Friends)] into
        (Other Person, List[Sorted Reccommendations])
