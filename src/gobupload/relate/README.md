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
- De aaneengesloten toestanden bepalen waarin de relatie heeft bestaan
- Dat levert start- en eindtoestanden op van de relatie
- De ingangsdatum van een relatie is max(begin geldigheid(starttoestanden))
- De einddatum van de relatie is het min(eind_geldigheid(eindtoestanden))

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
