import sys
import re

if __name__ == "__main__":

    meilleur_entite_pagerank = "Aucun"
    meilleur_pagerank = 0.0

    deuxieme_meilleur_entite_pagerank = "Aucun"
    deuxieme_meilleur_pagerank = 0.0

    troisieme_meilleur_entite_pagerank = "Aucun"
    troisieme_meilleur_pagerank = 0.0

    # Expression régulière mise à jour pour gérer les entités entourées de << >> au lieu de < >
    motif = r"<<([^>]+)>> <hasRank> \"([0-9\.]+)\" \."

    with open(str(sys.argv[1]), 'r') as fichier_donnees:
        for ligne in fichier_donnees.readlines():
            ligne = ligne.strip()  # Enlève les espaces blancs au début/à la fin
            print(f"Traitement de la ligne : {ligne}")  # Impression pour déboguer et voir quelle ligne est traitée

            # Utiliser l'expression régulière pour extraire l'entité et le pagerank
            correspondance = re.match(motif, ligne)
            if correspondance:
                entite = correspondance.group(1)  # Extrait l'URL ou l'entité
                entite_pagerank = float(correspondance.group(2))  # Extrait la valeur du pagerank

                print(f"Entité correspondante : {entite}, pagerank : {entite_pagerank}")  # Impression pour déboguer

                # Déterminer si c'est dans les 3 meilleurs pageranks
                if entite_pagerank > meilleur_pagerank:
                    troisieme_meilleur_entite_pagerank = deuxieme_meilleur_entite_pagerank
                    troisieme_meilleur_pagerank = deuxieme_meilleur_pagerank

                    deuxieme_meilleur_entite_pagerank = meilleur_entite_pagerank
                    deuxieme_meilleur_pagerank = meilleur_pagerank

                    meilleur_entite_pagerank = entite
                    meilleur_pagerank = entite_pagerank

                elif entite_pagerank > deuxieme_meilleur_pagerank:
                    troisieme_meilleur_entite_pagerank = deuxieme_meilleur_entite_pagerank
                    troisieme_meilleur_pagerank = deuxieme_meilleur_pagerank

                    deuxieme_meilleur_entite_pagerank = entite
                    deuxieme_meilleur_pagerank = entite_pagerank

                elif entite_pagerank > troisieme_meilleur_pagerank:
                    troisieme_meilleur_entite_pagerank = entite
                    troisieme_meilleur_pagerank = entite_pagerank

    print("1. L'entité " + str(meilleur_entite_pagerank) + " a le meilleur pagerank avec " + str(meilleur_pagerank))
    print("2. L'entité " + str(deuxieme_meilleur_entite_pagerank) + " a le deuxième meilleur pagerank avec " + str(deuxieme_meilleur_pagerank))
    print("3. L'entité " + str(troisieme_meilleur_entite_pagerank) + " a le troisième meilleur pagerank avec " + str(troisieme_meilleur_pagerank))
