*** Dragan Dan-Stefan 333 CA ***
*** Tema1 ASC ***


	Pentru implementarea temei am folosit 2 noi clase NodeThread, care
reprezinta nodul de management al ndoului cluster-ului (el se ocupa
de accesari la baza de date proprie nodului) si AccessThread, ale carei
obiecte reprezinta thread-uri de access create de un thread NodeThread,
pentru a face accese in baza de date a altor noduri.

	Ca elemente de sincronizare am folosit o coada pentru fiecare nod in
care voi pune rezultatele acesarii bazei de date de catre un thread de
tip AccessThread, un semafor pentru a limita numarul de threaduri de accesari
la max_pending_requests, o bariera reentranta (bariera din laboratorul 3),
pe care am folosit-o pentru a face asteptarea threadurilor la fiecare iteratie
in algoritmul de triunghiularizare a matricii si un event pe care il folosesc
la inceperea algoritmului de aflare a solutiilor unui sistem superior triungiular
de la ultimul nod si continuarea lui in ordine descrescatoare a node_id-ului.

	Pentru rezolvarea sistemului de ecuatii am folosit algoritmul de eliminare
gausiana fara pivotare.


