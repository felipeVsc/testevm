import pandas as pd
from progress.bar import Bar
listauf = []
siglas = [
                             'AC',
#                             'AL',
   #                          'AP',
 #                            'AM',
  #                           'BA',
    #                        'CE',
     #                       'DF',
      #                      'ES',
  #                          'GO',
#                           'MA',
                            'MS',
                            'MT',
                            'MG',
                            'PA',
                            'PB',
                            'PR',
       #                     'PE',
#                            'PI',
                            'RJ',
#                            'RN',
#                            'RS',
#                            'RO',
#                            'RR',
#                            'SC',
                            'SP',
#                            'SE',
#                            'TO',
                        ]


# arquivo = "dadosNovos/votacao_candidato_munzona_"+str(ano)+"_"+uf+".csv"
# reader = pd.read_csv(arquivo, sep=',',encoding='utf8',low_memory=False)
# reader.loc[x:x,'CD_CBO'] = CBO_list[x]
# reader.to_csv(arquivo, encoding='utf-8', index=False)
headerArquivoVotacao = ['DT_GERACAO','HH_GERACAO','ANO_ELEICAO','CD_TIPO_ELEICAO','NM_TIPO_ELEICAO','NR_TURNO','CD_ELEICAO','DS_ELEICAO','DT_ELEICAO','TP_ABRANGENCIA','SG_UF','SG_UE','NM_UE','CD_CARGO','DS_CARGO','SQ_CANDIDATO','NR_CANDIDATO','NM_CANDIDATO','NM_URNA_CANDIDATO','NM_SOCIAL_CANDIDATO','CD_SITUACAO_CANDIDATURA','DS_SITUACAO_CANDIDATURA','CD_DETALHE_SITUACAO_CAND','DS_DETALHE_SITUACAO_CAND','TP_AGREMIACAO','NR_PARTIDO','SG_PARTIDO','NM_PARTIDO','SQ_COLIGACAO','NM_COLIGACAO','DS_COMPOSICAO_COLIGACAO','DT_NASCIMENTO','NR_IDADE_DATA_POSSE','NR_TITULO_ELEITORAL_CANDIDATO','CD_GENERO','DS_GENERO','CD_OCUPACAO','DS_OCUPACAO','CD_SIT_TOT_TURNO','DS_SIT_TOT_TURNO','NR_ZONA','QT_VOTOS_NOMINAIS','CD_MUNICIPIO','CD_CBO']

def removerNulos(campo,valor):
    # se tudo certo, return True, se nao, retorna o que é p substituir na main
    
    if(type(valor)==str):
        if('#NE' in valor or '#NULO' in valor):
            # in porque tem momentos que nao é #NULO# e sim apenas #NULO sem o ultimo #
            return 'NULO'
        
    elif(pd.isna(valor) or type(valor)==int):
        #quando estiver como NaN o campo
        if (campo[0:2]=='CD' or campo[0:2]=='NR' or campo[0:2]=='SQ' or campo[0:2]=='QT'):
            #inteiro
            return -1
        else:
            return 'NULO'
    
        
    return True


ano = 2016
for uf in siglas:
    print(uf)
    try:
        candidatos = []
        votosLista = []
        print("readerVotacao")

        #readerVotacao = arquivo original

        arquivoVotacao = './2016org/votacao_candidato_munzona_'+str(ano)+'_'+uf+'.csv'
        readerVotacao = pd.read_csv(arquivoVotacao,encoding='latin1',sep=',')

        #readerNovo = arquivoNovo

        arquivoNovo = './2016/votacao_candidato_munzona_'+str(ano)+'_'+uf+'.csv'
        readerNovo = pd.read_csv(arquivoNovo,encoding='latin1',sep=',')

        bar = Bar('Processing', max=len(readerVotacao))

        # preencheu as coisas aqui, entao só rodar a mudança
        for y in range(len(readerVotacao)):
            sequencial = readerVotacao['SQ_CANDIDATO'][y]
            votos = readerVotacao['QT_VOTOS_NOMINAIS'][y]
            turno = readerVotacao['NR_TURNO'][y]
            if(sequencial not in candidatos):
                candidatos.append(sequencial)
                votosLista.append(votos)
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

        print('pos dicionario, readerNovo')
        dicionarioVotos = {key: value for key,value in zip(candidatos,votosLista)}
        readerNovo['QT_VOTOS_NOMINAIS'] = readerNovo['SQ_CANDIDATO'].map(dicionarioVotos)
        for z in range(len(readerNovo)):
            #limpeza de nulos
            
            for campoVotacao in headerArquivoVotacao:
                valor = readerNovo[campoVotacao][z]
                
                retorno = removerNulos(campoVotacao, valor)
                if(retorno!=True):
                    readerNovo.loc[z:z,campoVotacao] = retorno
            bar.next()
        bar.finish()
        del readerNovo['NR_ZONA']
        readerNovo.to_csv(arquivoNovo,encoding='utf8',sep=',',index=False)
    except FileNotFoundError:
        print('file not found')
        listauf.append(uf)
