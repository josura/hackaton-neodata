# Description: This file contains the functions to extract the general schema of the xml files and to convert the xml files into a pandas dataframe
import pandas as pd

# get leaf nodes of the xml tree at the root as a list of lists
def getLeafNodes(root):
    leafNodes = []
    if len(root) == 0:
        tag = root.tag.split("}")[1] # remove url from the tag
        return []
    for child in root:
        list_child = getLeafNodes(child)
        if len(list_child) == 0:
            leafNodes.append(child.tag.split("}")[1])
        else:
            leafNodes.append([child.tag.split("}")[1], list_child])
    return leafNodes

# get leaf nodes of the xml tree at the root item as a list of strings ( the path is separated by _ until a leaf node is reached)
def getLeafNodesString(root):
    leafNodes = []
    if len(root) == 0:
        tag = root.tag.split("}")[1] # remove url from the tag
        return ""
    for child in root:
        list_child = getLeafNodesString(child)
        if len(list_child) == 0:
            leafNodes.append(child.tag.split("}")[1])
        else:
            for child_str in list_child:
                leafNodes.append(child.tag.split("}")[1] + "_" + child_str)
    return leafNodes


# intersection of the schemas inside the xml files items
def xml2generalSchemaIntersection(root):
    generalSchema = getLeafNodesString(root[0]) # get the first schema
    for i in range(1, len(root)):
        schema = getLeafNodesString(root[i])
        generalSchema = list(set(generalSchema).intersection(set(schema)))
    return generalSchema

# union of the schemas inside the xml files items
def xml2generalSchemaUnion(root):
    generalSchema = getLeafNodesString(root[0]) # get the first schema
    for i in range(1, len(root)):
        schema = getLeafNodesString(root[i])
        generalSchema = list(set(generalSchema).union(set(schema)))
    return generalSchema



def xml2dfSpecific(root):
    all_records = []
    for record in root:
        record_dict = {}
        record_dict["Identificativo"] = record.find("Identificativo").text
        record_dict["Erogatore-CodiceIstituto"] = record.find("Erogatore").find("CodiceIstituto").text
        record_dict["Entrata"] = record.find("Entrata").find("Data").text + " " + record.find("Entrata").find("Ora").text
        record_dict["ModalitaArrivo"] = record.find("ModalitaArrivo").text
        record_dict["ResponsabileInvio"] = record.find("ResponsabileInvio").text
        record_dict["ProblemaPrincipale"] = record.find("ProblemaPrincipale").text
        record_dict["Triage"] = record.find("Triage").text
        record_dict["Assistito-CUNI"] = record.find("Assistito").find("CUNI").text
        record_dict["Assistito-ValiditaCI"] = record.find("Assistito").find("ValiditaCI").text
        record_dict["Assistito-TipologiaCI"] = record.find("Assistito").find("TipologiaCI").text
        record_dict["Assistito-CodiceIstituzioneTEAM"] = record.find("Assistito").find("CodiceIstituzioneTEAM").text
        record_dict["Assistito-DatiAnagrafici-Eta-Nascita"] = record.find("Assistito").find("DatiAnagrafici").find("Eta").find("Nascita").find("Anno").text + " " + record.find("Assistito").find("DatiAnagrafici").find("Eta").find("Nascita").find("Mese").text
        record_dict["Assistito-DatiAnagrafici-Genere"] = record.find("Assistito").find("DatiAnagrafici").find("Genere").text
        record_dict["Assistito-DatiAnagrafici-Cittadinanza"] = record.find("Assistito").find("DatiAnagrafici").find("Cittadinanza").text
        record_dict["Assistito-DatiAnagrafici-Residenza-Regione"] = record.find("Assistito").find("DatiAnagrafici").find("Residenza").find("Regione").text
        record_dict["Assistito-DatiAnagrafici-Residenza-Comune"] = record.find("Assistito").find("DatiAnagrafici").find("Residenza").find("Comune").text
        record_dict["Assistito-DatiAnagrafici-Residenza-ASL"] = record.find("Assistito").find("DatiAnagrafici").find("Residenza").find("ASL").text
        record_dict["Assistito-Prestazioni-PresaInCarico"] = record.find("Assistito").find("Prestazioni").find("PresaInCarico").find("Data").text + " " + record.find("Assistito").find("Prestazioni").find("PresaInCarico").find("Ora").text
        record_dict["Assistito-Prestazioni-Diagnosi-DiagnosiPrincipale"] = record.find("Assistito").find("Prestazioni").find("Diagnosi").find("DiagnosiPrincipale").text
        record_dict["Assistito-Prestazioni-Prestazione-PrestazionePrincipale"] = record.find("Assistito").find("Prestazioni").find("Prestazione").find("PrestazionePrincipale").text
        record_dict["Assistito-Prestazioni-Prestazione-PrestazioneSecondaria"] = record.find("Assistito").find("Prestazioni").find("Prestazione").find("PrestazioneSecondaria").text
        record_dict["Assistito-Dimissione-EsitoTrattamento"] = record.find("Assistito").find("Dimissione").find("EsitoTrattamento").text
        record_dict["Assistito-Dimissione-DataDest"] = record.find("Assistito").find("Dimissione").find("DataDest").text + " " + record.find("Assistito").find("Dimissione").find("OraDest").text
        record_dict["Assistito-Dimissione-Data"] = record.find("Assistito").find("Dimissione").find("Data").text + " " + record.find("Assistito").find("Dimissione").find("Ora").text
        record_dict["Assistito-Dimissione-LivelloAppropriatezzaAccesso"] = record.find("Assistito").find("Dimissione").find("LivelloAppropriatezzaAccesso").text
        record_dict["Importo-RegimeErogazione"] = record.find("Importo").find("RegimeErogazione").text
        record_dict["Importo-Lordo"] = record.find("Importo").find("Lordo").text
        record_dict["Importo-PosizioneAssistitoTicket"] = record.find("Importo").find("PosizioneAssistitoTicket").text
        record_dict["TipoTrasmissione"] = record.find("TipoTrasmissione").text
        all_records.append(record_dict)
    return pd.DataFrame(all_records)

# convert xml to pandas dataframe, using the general schema from the union of the schemas of the xml files
def xml2dfUnion(root):
    generalSchema = xml2generalSchemaUnion(root)
    all_items = []
    for item in root:
        item_dict = {}
        for feature in generalSchema:
            current_item = item
            feature_final_text = ""
            features_splitted = feature.split("_")
            leaf_node = features_splitted[-1]
            # check if the path of the feature exists in the item
            for current_node in features_splitted:
                # control if the current node is any of the nodes in the current item
                current_node_exists = False
                for child in current_item:
                    if child.tag.split("}")[1] == current_node:
                        current_node_exists = True
                        current_item = child
                        if current_node == leaf_node:
                            feature_final_text = current_item.text
                        break
                if not current_node_exists: # the node was not found in the item, so the feature is not present
                    break
            item_dict[feature] = feature_final_text
        all_items.append(item_dict)
    return pd.DataFrame(all_items)
