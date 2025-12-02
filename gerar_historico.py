import os
import pandas as pd
import numpy as np
import warnings

# Ignora avisos chatos de formatação
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

# ==============================================================================
# 1. CONFIGURAÇÃO
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DADOS_DIR = BASE_DIR 
SAIDA_DIR = os.path.join(BASE_DIR, 'relatorios_gerados')
ARQUIVO_SAIDA = os.path.join(SAIDA_DIR, 'DADOS_CONSOLIDADOS.csv')

os.makedirs(SAIDA_DIR, exist_ok=True)

print(">>> INICIANDO CONSOLIDAÇÃO 3.0 (AUTO-DETECT) <<<")
print(f"   Pasta: {DADOS_DIR}")

# ==============================================================================
# 2. FUNÇÃO DE LEITURA "RAIO-X"
# ==============================================================================
def carregar_csv_raio_x(caminho, alias):
    if not os.path.exists(caminho):
        return None
    
    nome_arq = os.path.basename(caminho)
    
    try:
        # TENTATIVA 1: Ler ignorando linhas de comentário (#)
        # Isso resolve o erro "Expected 1 fields in line 8" comum em arquivos da Copernicus/NOAA
        df = pd.read_csv(caminho, comment='#', sep=None, engine='python')
        
        # Limpa nomes das colunas
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        # Verifica se o cabeçalho foi lido corretamente (procura 'time' ou 'date')
        col_time = next((c for c in df.columns if 'time' in c or 'date' in c), None)
        
        # CENÁRIO B: Arquivo sem cabeçalho (A primeira linha já é dado ex: 1981-01-01...)
        if not col_time:
            # Verifica se a primeira coluna da primeira linha parece uma data
            try:
                pd.to_datetime(df.columns[0])
                # Se funcionou, é porque o cabeçalho é, na verdade, a primeira linha de dados!
                # Vamos recarregar o arquivo dizendo que NÃO tem cabeçalho
                df = pd.read_csv(caminho, comment='#', sep=None, engine='python', header=None)
                
                # Se tiver 2 colunas, assumimos [Data, Valor]
                if len(df.columns) == 2:
                    df.columns = ['time', 'valor']
                # Se tiver 3 ou mais, assumimos [Data, Lat, Lon, Valor...]
                elif len(df.columns) >= 3:
                    # Tenta adivinhar. Geralmente a última é o valor.
                    cols = ['time', 'lat', 'lon', 'valor']
                    if len(df.columns) > 4: # Caso tenha mais colunas
                         cols = [f'col_{i}' for i in range(len(df.columns))]
                         cols[0] = 'time'
                         cols[-1] = 'valor' # Chute: última coluna é o dado
                    df.columns = cols[:len(df.columns)]
                
                col_time = 'time'
            except:
                pass

        # Seleção da Coluna de Valor (Se ainda não tivermos achado)
        col_valor = None
        if 'valor' in df.columns:
            col_valor = 'valor'
        else:
            # Pega a primeira coluna que não seja metadado
            ignorar = ['time', 'date', 'lat', 'lon', 'depth', 'station', 'utc', 'elevation']
            candidatas = [c for c in df.columns if not any(x in c for x in ignorar)]
            if candidatas:
                col_valor = candidatas[0] # Pega a primeira coluna "estranha" como sendo o valor
        
        # Se achamos Data e Valor, processa
        if col_time and col_valor:
            # Converte Data
            df['data_ref'] = pd.to_datetime(df[col_time], errors='coerce').dt.normalize()
            
            # Converte Valor (força numérico)
            df[col_valor] = pd.to_numeric(df[col_valor], errors='coerce')
            
            # Remove datas inválidas
            df = df.dropna(subset=['data_ref'])
            
            # Agrupa por dia (Média)
            df_agrupado = df.groupby('data_ref')[col_valor].mean()
            
            print(f"      -> OK: {nome_arq} ({len(df_agrupado)} dias)")
            return df_agrupado.to_frame(name='valor')
        
        print(f"      [AVISO] Estrutura desconhecida em {nome_arq}. Cols: {list(df.columns)}")
        return None
            
    except Exception as e:
        print(f"      [ERRO] Falha ao ler {nome_arq}: {e}")
        return None

# ==============================================================================
# 3. EXECUÇÃO
# ==============================================================================
def main():
    # Lista de arquivos para processar
    # Chaves = Nome da coluna final no Excel
    # Valores = Lista de arquivos possíveis (Recente tem prioridade se aparecer primeiro na lista?)
    # Na verdade, vamos carregar 'hist' e 'rec' explicitamente.
    
    VARIAVEIS = ['sst', 'dhw', 'irradiancia', 'turbidez', 'salinidade', 'ph', 'nitrato', 'clorofila', 'oxigenio']
    
    # Mapeamento de nomes de arquivo
    # (Adicione variações aqui se necessário)
    ARQUIVOS_MAP = {
        'sst': ['sst.csv', 'sst_recente.csv', 'temperatura.csv'],
        'dhw': ['dhw.csv', 'dhw_recente.csv'],
        'irradiancia': ['par.csv', 'par_recente.csv'],
        'turbidez': ['turbidez.csv', 'turbidez_recente.csv'],
        'salinidade': ['salinidade.csv', 'salinidade_recente.csv'],
        'ph': ['ph.csv', 'ph_recente.csv'],
        'nitrato': ['nitrato.csv', 'nitrato_recente.csv'],
        'clorofila': ['clorofila.csv', 'clorofila_recente.csv'],
        'oxigenio': ['oxigenio.csv', 'oxigenio_recente.csv']
    }

    df_mestre = pd.DataFrame(columns=['data'])

    for var in VARIAVEIS:
        print(f"   ... Buscando dados de {var.upper()}")
        
        possiveis = ARQUIVOS_MAP.get(var, [])
        df_var_final = None
        
        # Tenta carregar cada arquivo da lista
        for nome_arq in possiveis:
            caminho = os.path.join(DADOS_DIR, nome_arq)
            df_temp = carregar_csv_raio_x(caminho, var)
            
            if df_temp is not None:
                if df_var_final is None:
                    df_var_final = df_temp
                else:
                    # Junta com o que já temos (priorizando o que já foi carregado ou o novo? 
                    # Vamos assumir que se tivermos 2 arquivos, queremos UNIR as datas)
                    df_var_final = df_var_final.combine_first(df_temp)
        
        if df_var_final is not None:
            # Renomeia e junta ao mestre
            df_var_final = df_var_final.rename(columns={'valor': var})
            
            if df_mestre.empty:
                df_mestre = df_var_final
            else:
                df_mestre = pd.merge(df_mestre, df_var_final, left_index=True, right_index=True, how='outer')
        else:
            print(f"       -> Nenhum dado válido encontrado para {var}")

    # --- LIMPEZA FINAL ---
    if df_mestre.empty:
        print("\n[ERRO CRÍTICO] Nenhum dado gerado. Verifique se os arquivos .csv estão nesta pasta.")
        return

    print("\n   Aplicando limpeza de linhas zeradas...")
    df_mestre.index.name = 'data'
    df_mestre = df_mestre.reset_index().sort_values('data')

    # Regra: Se qualquer variável (exceto DHW) for 0 ou Vazio, remove a linha.
    cols_verificar = [c for c in df_mestre.columns if c not in ['data', 'dhw']]
    
    # Marca linhas ruins
    linhas_ruins = df_mestre[cols_verificar].isin([0, 0.0, np.nan]).any(axis=1)
    
    # Filtra
    df_limpo = df_mestre[~linhas_ruins].copy()
    
    # Estatísticas
    total = len(df_mestre)
    validas = len(df_limpo)
    
    # Salva
    df_limpo.to_csv(ARQUIVO_SAIDA, index=False)
    
    print("-" * 40)
    print(f"RELATÓRIO FINAL:")
    print(f"Linhas Totais (Brutas): {total}")
    print(f"Linhas Válidas (Limpas): {validas}")
    print(f"Linhas Removidas: {total - validas}")
    print(f"Arquivo: {ARQUIVO_SAIDA}")
    print("-" * 40)

if __name__ == "__main__":
    main()