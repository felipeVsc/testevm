"""
Esse código aqui já vai servir para 2020 em diante.
Não é o mesmo código daquele que eu usei para tratar os anos anteriores, porque acho que vai ficar poluido
dada as variações que tem em cada ano.

Então, esse daqui já vai sevir pro padrão que o TSE, espero, deve manter nos próximos anos.

Pretendo rodar tudo dentro de um unico for x in range(len(reader)) apra não precisar ficar rodando
entre todos os dados de uma vez só

entao fiz algumas modificações em certas funcoes.

# Consulta cand tem que fazer a validacao e depois a adiçao de coisas

# votacaoNominal tem que fazer apenas a Validacao

Após rodar esse script daqui, rodar o de consolidação, que vai gerar o final.

Precisa ter na pasta os arquivos: list_canonicos, municipios, e grupos.csv

"""
import time
start_time = time.time()

import pandas as pd
import json
from unidecode import unidecode
from progress.bar import Bar
# progress bar


#siglas das UF

# RECOLOCAR AQUI DPS
siglas = [
                            'AC',
                            'AL',
                            'AP',
                            'AM',
                            'BA',
                            'CE',
                            'DF',
                            'ES',
                            'GO',
                            'MA',
                            'MS',
                            'MT',
                            'MG',
                            'PA',
                            'PB',
                            'PR',
                            'PE',
                            'PI',
                            'RJ',
                            'RN',
                            'RS',
                            'RO',
                            'RR',
                            'SC',
                            'SP',
                            'SE',
                            'TO',
                        ]

headerArquivoCandidato = ["DT_GERACAO","HH_GERACAO","ANO_ELEICAO","CD_TIPO_ELEICAO","NM_TIPO_ELEICAO",
    "NR_TURNO","CD_ELEICAO","DS_ELEICAO","DT_ELEICAO","TP_ABRANGENCIA","SG_UF","SG_UE","NM_MUNICIPIO","CD_CARGO","DS_CARGO","SQ_CANDIDATO","NR_CANDIDATO","NM_CANDIDATO",
    "NM_URNA_CANDIDATO","NM_SOCIAL_CANDIDATO","NR_CPF_CANDIDATO","NM_EMAIL","CD_SITUACAO_CANDIDATURA",
    "DS_SITUACAO_CANDIDATURA","CD_DETALHE_SITUACAO_CAND","DS_DETALHE_SITUACAO_CAND","TP_AGREMIACAO",
    "NR_PARTIDO","SG_PARTIDO","NM_PARTIDO","SQ_COLIGACAO","NM_COLIGACAO","DS_COMPOSICAO_COLIGACAO",
    "CD_NACIONALIDADE","DS_NACIONALIDADE","SG_UF_NASCIMENTO","CD_MUNICIPIO_NASCIMENTO","NM_MUNICIPIO_NASCIMENTO",
    "DT_NASCIMENTO","NR_IDADE_DATA_POSSE","NR_TITULO_ELEITORAL_CANDIDATO","CD_GENERO","DS_GENERO","CD_GRAU_INSTRUCAO",
    "DS_GRAU_INSTRUCAO","CD_ESTADO_CIVIL","DS_ESTADO_CIVIL","CD_COR_RACA","DS_COR_RACA","CD_OCUPACAO","DS_OCUPACAO",
    "VR_DESPESA_MAX_CAMPANHA","CD_SIT_TOT_TURNO","DS_SIT_TOT_TURNO","ST_REELEICAO","ST_DECLARAR_BENS","NR_PROTOCOLO_CANDIDATURA",
    "NR_PROCESSO","CD_SITUACAO_CANDIDATO_PLEITO","DS_SITUACAO_CANDIDATO_PLEITO","CD_SITUACAO_CANDIDATO_URNA","DS_SITUACAO_CANDIDATO_URNA","ST_CANDIDATO_INSERIDO_URNA"
]

headerArquivoVotacao = ["DT_GERACAO","HH_GERACAO","ANO_ELEICAO","CD_TIPO_ELEICAO","NM_TIPO_ELEICAO","NR_TURNO","CD_ELEICAO","DS_ELEICAO","DT_ELEICAO","TP_ABRANGENCIA","SG_UF","SG_UE","NM_UE","CD_MUNICIPIO","NM_MUNICIPIO","NR_ZONA","CD_CARGO","DS_CARGO","SQ_CANDIDATO","NR_CANDIDATO","NM_CANDIDATO","NM_URNA_CANDIDATO","NM_SOCIAL_CANDIDATO","CD_SITUACAO_CANDIDATURA","DS_SITUACAO_CANDIDATURA","CD_DETALHE_SITUACAO_CAND","DS_DETALHE_SITUACAO_CAND","TP_AGREMIACAO","NR_PARTIDO","SG_PARTIDO","NM_PARTIDO","SQ_COLIGACAO","NM_COLIGACAO","DS_COMPOSICAO_COLIGACAO","CD_SIT_TOT_TURNO","DS_SIT_TOT_TURNO","ST_VOTO_EM_TRANSITO","QT_VOTOS_NOMINAIS"]




# arquivo dos municipios
arquivoMunicipios = 'municipios/municipiosjson.json'



# correção da data #

def retornarPadraoNovo(anos,meses,dias):
    #retornar yyyy-mm-dddd

    if(len(dias)<2):
        dias = '0'+dias
    elif(len(meses)<2):
        meses = '0'+meses
    elif(len(anos)<4):
        if(int(anos)<10):
            #adiciona o 20 p ser 2002, 2001
            anos = '20'+anos
        else:
            anos = '19'+anos

    novaData = anos+'-'+meses+'-'+dias

    return novaData

def corrigirDataNascimento(entrada):
    # pega a data que tem, separa, envia pra quem vai retornar o novo padrao
    # e retorna a data ja com o novo padrao pra substituir na execucao (main)

    data = entrada
    
    if(pd.isna(data)==False and data!='NULO' and '-' not in data):
        listaDatas = data.split('/')

        dias = listaDatas[0]
        meses = listaDatas[1]
        anos = listaDatas[2]

        novaData = retornarPadraoNovo(anos, meses, dias)
        return novaData
    else:
        return -1
    reader.to_csv(arquivo, encoding='utf-8', index=False) 


# cbo #

def cboInsert(c): 
    # c = ocupacap
    # vai receber a ocupacao e vai retornar o CBO Dele
    
    
    cbo = pd.read_csv('lista_canonicos.csv', encoding='ISO-8859-1', ) 
    was_f = False   # Checar se o indice jah foi verificado, se for encontrado muda pra true
    for y in cbo["termo"]:  # Checar a existencia na base da cbo pela descricao
        if str(c).lower() == str(y).lower(): # Deixa as strings em caixa baixa e compara
            idx = cbo[cbo["termo"] == y].index.tolist() # Caso exista, pega o indice pra acessar o codigo referente
            return cbo.iloc[idx[0]]["codigo"] # Adiciona o codigo
            was_f = True # Marca como encontrado
            break # Vai pro for externo

    if c == "OUTROS":
        return 'OUTROS'
    elif not was_f: # Quando nao encontrado eh por nao existir na CBO
        return 'OUTROS'


# remoção de campos nulos #

def removerNulos(campo,valor,reader):
    # se tudo certo, return True, se nao, retorna o que é p substituir na main
    
    if(type(valor)==str):
        if('#NE' in valor or '#NULO' in valor):
            # in porque tem momentos que nao é #NULO# e sim apenas #NULO sem o ultimo #
            return 'NULO'
        
    elif(pd.isna(valor) or type(valor)==int):
        #quando estiver como NaN o campo
        if (campo[0:2]=='CD' or campo[0:2]=='NR' or campo[0:2]=='SQ'):
            #inteiro
            return -1
        else:
            return 'NULO'
    
        
    return True
        

# titulo de eleitor #

def completarZerosTitulo(titulo):
        # se len(titulo) < 12, entao completa com 12 digitos 
    
        numeroTitulo = str(titulo)
        tamanho = 12-len(numeroTitulo)
        return (tamanho*'0')+numeroTitulo

def padronizarTituloEleitor(tituloEleitor):
    # faz a padronização
    if(len(str(tituloEleitor))!=12):
        return completarZerosTitulo(tituloEleitor)

            
#genero#    
def inferirSexoPeloNome(nome):
        # 2 = m / 4 = f
        
        nomes = pd.read_csv("grupos.csv")
        alternativos = nomes['names']
        listasNomes = alternativos.str.split('|')
        nome = unidecode(nome)
        
        for x in range(len(listasNomes)):
            if(nome in listasNomes[x]):
                sexo = nomes['classification'][x]
                if(sexo=='M'):
                    return 2
                elif(sexo=='F'):
                    return 4
                else:
                    return -1
                break

def temCampoSexo(sexo,nome):
    # procura pra ver se o campo genero esta populado
    # retorna uma lista com o cd_gen e o ds_gen
    # se tudo estiver correto, ele retorna True

    if(sexo=='FEMININO' or sexo=='MASCULINO'):
        return True
    else:

        listaNomesCandidatos = nome.split(' ')
        sexo = inferirSexoPeloNome(listaNomesCandidatos[0])

        if(sexo==2):
            return [2,'MASCULINO']
        elif(sexo==4):
            return [4,'FEMININO']
        else:
            return [-1,'NULO']

           

#utf 8#
def replaceUTF8(reader,header):
    # passando e retornando o reader com o novo reader

    for y in header:
        
        if(y[0:2] in ['DS','NM','ST','TP','SG']):
            if(y=='SG_UE' or y=='DS_SIT_CANDIDATO'):
                pass
            else:
                try:
                    reader[y] = reader[y].apply(unidecode)
                except KeyError as key:
                    pass


    return reader

        

# idade #
def corrigirIdade(idade,datanascimento,anoeleicao):
    #se idade==-1 or idade!=int
    # retorna a idade nova
        
    tipoVariavel = type(idade.item())
    if(tipoVariavel==int):
    
        # tem erro aqui, logo pegar data nascimento e fazer idade
        anoNascimentoPrimario = str(datanascimento)

        try:
            if(type(anoNascimentoPrimario)!=str):
                anoNascimentoPrimario = str(int(anoNascimentoPrimario))
            
            anoNascimento = int(anoNascimentoPrimario[-2:])+1900 # pegar os 4 ultimos

            idadenova = anoeleicao.item()-anoNascimento
                                
            return idadenova
        except:
            return -1



#cod ibge#

def retornarTudoCodigos(codigotse,nomemunicipio):
        """
            
        """
        with open(arquivoMunicipios, 'r',encoding='utf8') as myfile:
            data=myfile.read()

        dados = []    
        nomeDoMunicipio = unidecode(str(nomemunicipio))
        obj = json.loads(data)
        for entry in obj:
            if(nomeDoMunicipio==""):
                # entao pegar pelo codigo tse
                if entry['codigo_tse']==codigotse:
                    dados.append(entry['codigo_tse'])
                    dados.append(entry['codigo_ibge'])
                    dados.append(unidecode(entry['nome_municipio']))
                else:
                    dados.append(-1)
                    dados.append(-1)
                    dados.append('NULO')
            
            else:
                #pegar entao pelo nomeMun

                if unidecode(entry['nome_municipio'])==nomeDoMunicipio:
                    dados.append(entry['codigo_tse'])
                    dados.append(entry['codigo_ibge'])
                    dados.append(unidecode(entry['nome_municipio']))
                else:
                    dados.append(-1)
                    dados.append(-1)
                    dados.append('NULO')

            
        return dados


# def addCodMunIBGE_codTSE(reader):
    

#         listaDasCoisas = []

#         for x in range(len(reader)):
#             if(type(reader['SG_UE'][0])!=str):
#                 ### QUANDO TIVER SG_UE COMO CODIGO DE TSE ###
#                 codMunTSE = reader['SG_UE'][x]
#                 listaInformacoes = retornarTudoCodigos(codMunTSE, "")

#                 codIbgeMun = listaInformacoes[1]
#                 append = listaDasCoisas.append
#                 append(codIbgeMun)
            

#             if(reader['CD_MUNICIPIO'][x]==-1):
                    
#                 nomecidade = reader['NM_MUNICIPIO'][x]
#                 listaInfo = retornarTudoCodigos("", nomecidade)
                
#                 reader.loc[x:x,'CD_MUNICIPIO'] = listaInfo[0]

#         if(len(listaDasCoisas)!=0):
#             reader['CD_MUN_IBGE'] = listaDasCoisas
        

#         return reader

    

def main(ano):
    #arquivo votacao eh o que vai ser o final. 

    #primeiro faz no candidato, com adicao + validacao
    # faz depois votacao com apenas validacao
    # no final a consolidação é quem vai juntar as duas


    for uf in ['BA']:
        print(uf)
        try:
            arquivoCandidato = 'consulta_cand_'+str(ano)+'_'+uf+'.csv'
            
            readerCandidato = pd.read_csv(arquivoCandidato,encoding='latin1',sep=';')
            # readerCandidato = pd.read_csv(arquivoCandidato,encoding='utf8',sep=',')
            readerCandidato = readerCandidato.rename(columns={"NM_UE": "NM_MUNICIPIO"})


        # CONSULTA CAND ##

        # VALIDACAO #
            print("INICIO VALIDACAO CANDIDATOS")

            readerCandidato = replaceUTF8(readerCandidato,headerArquivoCandidato)
            

            readerCandidato['CD_CBO'] = 0 # so criando aqui a coluna
            bar = Bar('Processing', max=len(readerCandidato))
            for x in range(len(readerCandidato)):
                dataNascimento = readerCandidato['DT_NASCIMENTO'][x]
                idade = readerCandidato['NR_IDADE_DATA_POSSE'][x]
                tituloEleitor = readerCandidato['NR_TITULO_ELEITORAL_CANDIDATO'][x]
                genero = readerCandidato['DS_GENERO'][x]
                anoEleicao = readerCandidato['ANO_ELEICAO'][0]
                nome = readerCandidato['NM_CANDIDATO'][x]
                ocupacao = readerCandidato['DS_OCUPACAO'][x]
                
                readerCandidato.loc[x:x,'DT_NASCIMENTO'] = corrigirDataNascimento(dataNascimento)
                
                readerCandidato.loc[x:x,'NR_IDADE_DATA_POSSE'] = corrigirIdade(idade, dataNascimento, anoEleicao)

                readerCandidato.loc[x:x,'NR_TITULO_ELEITORAL_CANDIDATO'] = padronizarTituloEleitor(tituloEleitor)

                listaSexo = temCampoSexo(genero, nome)
                if(listaSexo!=True):
                    cdGen = listaSexo[0]
                    dsGen = listaSexo[1]
                    readerCandidato.loc[x:x,'CD_GENERO'] = cdGen
                    readerCandidato.loc[x:x,'DS_GENERO'] = dsGen
                

        #ADICAO #
                readerCandidato.loc[x:x,'CD_CBO'] = cboInsert(ocupacao)
                # salvo aqui o readerCandidato
                readerCandidato.to_csv(arquivoCandidato,sep=',',encoding='utf8',index=False)


        # NULOS #

                for campoCandidato in headerArquivoCandidato:
                    valor = readerCandidato[campoCandidato][x]
                    retorno = removerNulos(campoCandidato, valor, readerCandidato)
                    if(retorno!=True):
                        readerCandidato.loc[x:x,campoCandidato] = retorno
                
                bar.next()
            bar.finish()

            readerCandidato.to_csv(arquivoCandidato,sep=',',encoding='utf8',index=False)
        #correcao quantidade votos #
            
            print("FIM VALIDACAO CANDIDATOS")
            print("INICIO VALIDACAO VOTACAO")
        # VOTACAO NOMINAL ##
            """
                Aqui tem a questão dos votos, porque como são zonas, então o mesmo candidato
                aparece várias vezes no arquivo, e tem que fazer a soma dos votos de cada zona
                para aquele candidaot

                Ai ele vai montar um dicionário com a quantidade de votos de cada candidato e enquanto
                monta o dicionario, vai apagando as duplicatas, pra no final nao termos o total de votos
                logo em vez de um voto por zona eleitoral.

                Depois ele vai limpar os nulos e etc etc

                e no final vai rodar novamente para passar atualizando os votos de acordo com o dicionario.
            """
            arquivoVotacao = 'votacao_candidato_munzona_'+str(ano)+'_'+uf+'.csv'
                
            readerVotacao = pd.read_csv(arquivoVotacao,encoding='latin1',sep=';')
            readerVotacao = replaceUTF8(readerVotacao,headerArquivoVotacao)
            readerVotacao = addCodMunIBGE_codTSE(readerVotacao)

            readerVotacao['NR_TITULO_ELEITORAL_CANDIDATO'] = 0
            readerVotacao['NR_IDADE_DATA_POSSE'] = 0
            readerVotacao['CD_GENERO'] = 0
            readerVotacao['DS_GENERO'] = ""
            readerVotacao['CD_CBO'] = 0
            candidatos = []
            votosLista = []
            print("DICIONARIO QT_VOTOS_NOMINAIS")
            
            # arquivo original #
            bar = Bar('Processing', max=len(readerVotacao))
            for y in range(len(readerVotacao)):
                
                sequencial = readerVotacao['SQ_CANDIDATO'][y]
                votos = readerVotacao['QT_VOTOS_NOMINAIS'][y]
                turno = readerVotacao['NR_TURNO'][y]
                append = candidatos.append
                if(sequencial not in candidatos):
                    append(sequencial)
                    append = votosLista.append
                    append(votos)
                else:
                    if(pd.isna(votos)==False and votos!=""):
                        indiceCandidatos = candidatos.index(sequencial)
                        votosLista[indiceCandidatos]+=votos
                        readerVotacao = readerVotacao.drop(y)
                    else:
                        indiceCandidatos = candidatos.index(sequencial)
                        votosLista[indiceCandidatos]+=0
                        readerVotacao = readerVotacao.drop(y)
                bar.next()
            bar.finish()

            
            
            
            readerVotacao.to_csv(arquivoVotacao,sep=',',encoding='utf8', index=False)
            
            print("TROCA QT_VOTOS_NOMINAIS + Limpeza Nulos")
            bar = Bar('Processing', max=len(readerVotacao))
            readerVotacao = pd.read_csv(arquivoVotacao,sep=',',encoding='utf8')
            ## new ##

            # baseado no sq_candidato
            dicionarioCdGen = {key: value for key,value in zip(readerCandidato['SQ_CANDIDATO'],readerCandidato['CD_GENERO'])}
            dicionarioDsGen = {key: value for key,value in zip(readerCandidato['SQ_CANDIDATO'],readerCandidato['DS_GENERO'])}
            dicionarioTitulo = {key: value for key,value in zip(readerCandidato['SQ_CANDIDATO'],readerCandidato['NR_TITULO_ELEITORAL_CANDIDATO'])}
            dicionarioIdade = {key: value for key,value in zip(readerCandidato['SQ_CANDIDATO'],readerCandidato['NR_IDADE_DATA_POSSE'])}
            dicionarioCbo = {key: value for key,value in zip(readerCandidato['SQ_CANDIDATO'],readerCandidato['CD_CBO'])}
            
            readerVotacao['CD_GENERO'] = readerVotacao['SQ_CANDIDATO'].map(dicionarioCdGen)
            readerVotacao['DS_GENERO'] = readerVotacao['SQ_CANDIDATO'].map(dicionarioDsGen)
            readerVotacao['NR_TITULO_ELEITORAL_CANDIDATO'] = readerVotacao['SQ_CANDIDATO'].map(dicionarioTitulo)
            readerVotacao['NR_IDADE_DATA_POSSE'] = readerVotacao['SQ_CANDIDATO'].map(dicionarioIdade)
            readerVotacao['CD_CBO'] = readerVotacao['SQ_CANDIDATO'].map(dicionarioCbo)
            # ai aqui seria só dar o mapping em cada um de votacao            
            dicionarioVotos = {key: value for key,value in zip(candidatos,votosLista)}
            readerVotacao['QT_VOTOS_NOMINAIS'] = readerVotacao['SQ_CANDIDATO'].map(dicionarioVotos)
## termino de new ##
            listaDasCoisas = []
            for z in range(len(readerVotacao)):
                #limpeza nulos

                    for campoVotacao in headerArquivoVotacao:
                            valor = readerVotacao[campoVotacao][z]
                            if(pd.isna(valor)==False):
                                retorno = removerNulos(campoVotacao, valor, readerVotacao)
                                if(retorno!=True):
                                    readerVotacao.loc[z:z,campoVotacao] = retorno
                    
                    dataEleicao = readerVotacao['DT_ELEICAO'][z]
                    
                    if(pd.isna(dataEleicao)==False):
                        readerVotacao.loc[z:z,'DT_ELEICAO'] = corrigirDataNascimento(dataEleicao)
                    else:
                        readerVotacao.loc[z:z,'DT_ELEICAO'] = -1

                # correcao dos votos
                    sequencialC = readerVotacao['SQ_CANDIDATO'][z]
                    
                # add codmun
                    if(type(readerVotacao['SG_UE'][0])!=str):
                    ### QUANDO TIVER SG_UE COMO CODIGO DE TSE ###
                        codMunTSE = readerVotacao['SG_UE'][z]
                        listaInformacoes = retornarTudoCodigos(codMunTSE, "")

                        codIbgeMun = listaInformacoes[1]
                        append = listaDasCoisas.append
                        append(codIbgeMun)
                    

                    if(readerVotacao['CD_MUNICIPIO'][z]==-1):
                            
                        nomecidade = readerVotacao['NM_MUNICIPIO'][z]
                        listaInfo = retornarTudoCodigos("", nomecidade)
                        
                        readerVotacao.loc[z:z,'CD_MUNICIPIO'] = listaInfo[0]
                     
               
                    bar.next()
            readerVotacao['CD_MUN_IBGE'] = listaDasCoisas
            readerVotacao.to_csv(arquivoVotacao,sep=',',encoding='utf8',index=False)
            bar.finish()
            print("FIM VALIDACAO VOTACAO")
        except FileNotFoundError:
            print("ARQUIVO NAO ENCONTRADO")

# ano = input('Digite o ano')
ano = 2020
main(ano)
print("--- %s seconds ---" % (time.time() - start_time))