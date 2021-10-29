import pandas as pd
from progress.bar import Bar
siglas = [
                            # 'AC',
                            # 'AL',
                            # 'AP',
                            # 'AM',
                            # 'BA',
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


# arquivo = "dadosNovos/votacao_candidato_munzona_"+str(ano)+"_"+uf+".csv"
# reader = pd.read_csv(arquivo, sep=',',encoding='utf8',low_memory=False)
# reader.loc[x:x,'CD_CBO'] = CBO_list[x]
# reader.to_csv(arquivo, encoding='utf-8', index=False)
headerArquivoVotacao = ['DT_GERACAO','HH_GERACAO','ANO_ELEICAO','NR_TURNO','SG_UF','SG_UE','NM_UE','CD_CARGO','DS_CARGO','NM_CANDIDATO','SQ_CANDIDATO','NR_CANDIDATO','NM_URNA_CANDIDATO','CD_SITUACAO_CANDIDATURA','DS_SITUACAO_CANDIDATURA','NR_PARTIDO','SG_PARTIDO','NM_PARTIDO','CD_LEGENDA','SG_LEGENDA','DS_COMPOSICAO_LEGENDA','NM_LEGENDA','CD_OCUPACAO','DS_OCUPACAO','DT_NASCIMENTO','NR_TITULO_ELEITORAL_CANDIDATO','NR_IDADE_DATA_POSSE','CD_GENERO','DS_GENERO','SG_UF_NASCIMENTO','CD_MUNICIPIO_NASCIMENTO','NM_MUNICIPIO_NASCIMENTO','CD_SIT_TOT_TURNO','DS_SIT_TOT_TURNO','CD_IBGE_MUN_NASC','NR_ZONA','CD_SIT_CAND_SUPERIOR','DS_SIT_CAND_SUPERIOR','QT_VOTOS_NOMINAIS','CD_TIPO_ELEICAO','TP_ABRANGENCIA','CD_MUNICIPIO','NM_SOCIAL_CANDIDATO','CD_DETALHE_SITUACAO_CAND','DS_DETALHE_SITUACAO_CAND','CD_CBO','CD_MUN_IBGE']

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


ano = 2002
for uf in siglas:
    print(uf)
    try:
        candidatos = []
        votosLista = []
        print("readerVotacao")

        #readerVotacao = arquivo original

        arquivoVotacao = 'dadosOriginais/votacao_candidato_munzona_'+str(ano)+'_'+uf+'.csv'
        readerVotacao = pd.read_csv(arquivoVotacao,encoding='latin1',sep=',')

        #readerNovo = arquivoNovo

        arquivoNovo = 'dadosNovos/votacao_candidato_munzona_'+str(ano)+'_'+uf+'.csv'
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
        pass