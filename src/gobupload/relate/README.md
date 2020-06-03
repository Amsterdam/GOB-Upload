# Relaties in GOB

## Basis idee

Relaties zijn vastgelegd in het GOB model, bijvoorbeeld:

```
     "wijken":
          "ligt_in_stadsdeel": {
            "type": "GOB.Reference",
            "description": "Het stadsdeel waar de wijk in ligt.",
            "ref": "gebieden:stadsdelen"
          }
```

In de catalogus gebieden is een wijk (uit de verzameling wijken) verbonden aan een stadsdeel (uit de verzameling stadsdelen) en deze relatie is vastgelegd in het attribuut ligt_in_stadsdeel.

Een relatie kan veranderen over tijd. Als bijvoorbeeld de gebiedsindeling van Amsterdam verandert kan een wijk in een ander stadsdeel komen te liggen.

Dergelijke veranderingen zijn in de onderliggende bronsystemen vastgelegd als toestanden. Een nieuwe gebiedsindeling leidt tot nieuwe toestanden van wijken en stadsdelen. Voor elke combinatie van toestanden kan de relatie wijk – stadsdeel worden berekend.

In het algemeen zijn relaties zijn in GOB gedefinieerd als:
```
Object x heeft in toestand n een relatie met Object y in toestand m
```
## Aanvankelijke aanpak

De relatie tussen twee objecten werd verdeeld in alle mogelijke tijdvakken.
```
Wijk      :    |--1---|--2--|--3-------->
Stadsdeel :   |-1-|-2-|--3--|--4--|--5-->
Resultaat :    |11|-12|--23-|--34-|--35->
```
Voor alle objecten in GOB werden voor alle toestanden alle relaties berekend.

## Aanpassing aanpak

De aanvankelijke aanpak leidde tot een weliswaar correct resultaat maar ook tot een uiterst omvangrijke dataset. Te omvangrijk en te gedetailleerd om verwerkbare resultaten te kunnen opleveren.

Historische bestanden die op basis van de historische relaties werden gegenereerd vergden veel tijd en leverden teveel details op om nog verder verwerkt te kunnen worden.

Er is daarom gekozen om van elk object alleen de laatst bekende relatie bij te houden.
```
Wijk      :    |--1---|--2--|--3-------->
Stadsdeel :   |-1-|-2-|--3--|--4--|--5-->
Resultaat :    |--12--|--23-|-----35---->
```
In het voorbeeld leidt dit tot minder relaties. Relaties 1-1 en 3-4 vervallen omdat de laatst bekende relatie van wijk in toestand 1 gelijk is aan stadsdeel in toestand 2.

Voor elke toestand van wijk wordt steeds het stadsdeel gezocht wat op het eind-geldigheid van de toestand geldig is.

## Ingangsdatum

De relaties worden nog steeds juist berekend. Weliswaar mist er historisch detail maar de geregistreerde relatie is juist.

Het probleem treedt op bij het bepalen van de ingangsdatum van de relatie:
```
Wijk      :    |--1---|--2--|--3-------->
Stadsdeel :   |-1-|-2-|--3--|--4--|--5-->
Resultaat :    |--12--|--23-|-----35---->
```
De ingangsdatum in de huidige GOB programmatuur is gelijk aan een van de ingangsdatums van de gerelateerde toestandsdatums.
In het eerdere voorbeeld is de ingangsdatum van relatie 3-5 of de ingangsdatum van wijk 3 of de ingangsdatum van stadsdeel 5.

Beide van voornoemde datums zijn onjuist:

De wijk is immers aansluitend gedurende al haar toestanden gerelateerd geweest aan het stadsdeel.
De ingangsdatum van de relatie zou dus gelijk moeten zijn aan de ingangsdatum van de wijk in toestand 1.
Het stadsdeel bestaat weliswaar al eerder maar dat is niet van belang omdat de relatie toen nog niet bestond.

Om de ingangs- en einddatum van een relatie te bepalen is het nodig om:
- De aaneengesloten toestanden* bepalen waarin de relatie heeft bestaan.
- Dat levert start- en eindtoestanden op van de relatie
- De ingangsdatum van een relatie is max(begin geldigheid(starttoestanden))
- De einddatum van de relatie is het min(eind_geldigheid(eindtoestanden))

Let op: aaneengesloten toestanden hoeven niet per definitie opeenvolgende volgnummers te hebben, maar de volgnummers dienen wel volgorderlijk te zijn.
Een object kan in GOB bijvoorbeeld de volgnummers 1, 2 en 5 hebben, waarbij 3 en 4 niet in GOB staan. Zolang de eind_geldigheid van 2 aansluit op de
begin_geldigheid van 5 worden 2 en 5 gezien als aaneengesloten toestanden.

## Nader voorbeeld
```
Wijk      :    |--1---|--2--|--3------|
Stadsdeel :   |-1-|-2-|      |--3--|-4-->
Resultaat :    |--12--|      |-----34--|
```

- Relatie 12 bestaat uit de opeenvolgende toestanden
  - wijk [1]
  - stadsdeel [1, 2]
- Starttoestanden zijn wijk [1] en stadsdeel [1]
- Eindtoestanden zijn wijk [1] en stadsdeel [2]
- begin = max(begin geldigheid(wijk [1], stadsdeel [1])) = begin geldigheid wijk [1]
- eind = min(eind geldigheid(wijk [1], stadsdeel [2])) = eind geldigheid wijk [1]

wijk [2] is niet gerelateerd aan een stadsdeel.

- Relatie 34 bestaat uit de opeenvolgende toestanden
  - wijk [3]
  - stadsdeel [3, 4]
- Starttoestanden zijn wijk [3] en stadsdeel [3]
- Eindtoestanden zijn wijk [3] en stadsdeel [4]
- begin = max(begin geldigheid(wijk [3], stadsdeel [3])) = begin geldigheid stadsdeel [3]
- eind = min(eind geldigheid(wijk [3], stadsdeel [4])) = eind geldigheid wijk [3]


# Updating relation tables

Updating the relation tables can be a an expensive process to execute.
Say that we have an entity A that contains a reference to entity B.
If we have 1000 entities A and 1000 entities B, there are 1 000 000
possible combinations to evaluate. This increases quadratically when A
or B increases, and linearly if we have a ManyReference instead of a
single Reference. In practice this means that we have ManyReferences
between collections that have 1M+ entities that result in relation
tables with many millions of rows. Evaluating many millions of rows is
expensive, and this is just for one relation.

Instead of evaluating every possible relation every single time, we
should only evaluate the combinations of objects A <> B that may have
changed. This README explains how.

## The relation table
First some information on how the relation table is set up, as this is
important for the implementation of updating the relation tables.
Say we have a collection with entities *A* with an attribute *a*
containing references *r* to a collection of entities *B*. For a
ManyReference from *A* to *B* an entity *A* can have 0 or more
references *r* to *B*, whereas a single Reference from *A* to *B* can
only contain 0 or 1 references *r*.

For every reference *r* in *A* we add a row to our relation table *R*.
The presence of this row does not ensure that a relation between *A* and
*B* exists; it is merely the possibility of a relation. In practice this
means that every row in relation table *R* has at least *src_id* and
*bronwaarde* set (possibly *src_volgnummer*). The columns *dst_id* and
*dst_volgnummer* are only set when a relation *ArB* between *A* and a
*B* is actually found. It follows that:

- For an attribute *a* in *A* of type (single)Reference, *|A|* >= *|R|*
- For an attribute *a* in *A* of type ManyReference,
*|R|* = avg_degree(*a*) x *|A|*.
- *R* includes all possible relations *ArB* and no more.

## The updating process, the theory
Say, we have 1000 entities *A* and 1000 entities *B*. For simplicity we
assume that an attribute *a* in *A* is of type Reference, so that for
each *A* we have at most 1 row in table *R*.

To evaluate only the possible relations *ArB* that may have been added,
updated or deleted, we need to determine which these are. Updating all
possible relations *ArB* means, by definition, updating all rows in
relation table *R*. This means that there are 1000 x 1000 = 1M possible
relations *ArB*.

Now, if only 10 entities *A* have been updated/added/deleted, and none
in *B*, it means that, for those 10 *A*:

- A1. the existing *ArB* originating from those *A* could have been
changed.
- A2. there could be new *ArB* originating from those *A*.
- A3. an existing *ArB* originating from those *A* could have been
deleted.
- A4. all other *A* can be left alone.

The other way around, if only 10 *B* have been updated/added/deleted,
and none *A*, it means that for those 10 *B*:

- B1. the existing *ArB* referring to those *B* could have been changed.
- B2. there could be new *ArB* referring to those *B*.
- B3. an existing *ArB* could now point to a deleted *B*.
- B4. all other *B* can be left alone.

To evaluate (and update) only the possibly changed(/added/deleted)
relations *ArB*, we know now that:

- When some *A* are updated, we only have to evaluate the updated *A*
with **all** *B*, as we could have created new relations *ArB* to
different *B*, and we need to evaluate all possibilities.
- When some *B* are updated, we have to evaluate **all** *A* with the
updated *B*.

It will often be the case however, that both some *A* and some *B* have
been updated, but instead of just relating all *A* with all *B* again,
and having to evaluate 1000 * 1000 = 1M possible *ArB*, we first update
all the necessary *A* and then all the necessary *B* following the rules
A1-4 and B1-4 above. This means that when 10 *A* and 10 *B* have been
updated, we only have to evaluate (10A * 1000B + 1000A * 10B) = 20 000
possible *ArB* instead of 1M.

## The updating process, in practice
To implement the theory above, we need to keep track of which entities
in both collections are updated. We do this with two columns in the
relation table *R*, *_last_src_event* and *_last_dst_event*. When a row
in *R* is updated, the *max(_last_event)(A)* and *max(_last_event)(B)*
are set in these columns. So when we query on *max(_last_src_event)* on
the relation table *R*, we know that the entities in *A* with an event
id greater than that are updated and need to be re-evaluated.
Note that if during a particular run of updating the table no rows in
*R* are updated, neither *_last_src_event* nor *_last_dst_event* will be
updated. This means that during the following run the same entities
will be evaluated again.

Because both *_last_src_event* and *_last_dst_event* are set on the
updated rows, the update action of the changed *A* and the changed *B*
must happen atomically; only one set of events will be generated
including all updates. In *update_table.py* this is implemented as the
UNION of the results of two queries. Also, updating all possible *ArB*
in one go ensures the idempotency of this process.

The first query checks all updated *A* with all *B*. The second query
then checks all remaining *A* with the updated *B*. Note that the first
query results in a list of (events on) *ArB*. By excluding the already
evaluated *A* in the second query, we prevent duplicate *ArB* (and we
can safely use UNION ALL).
