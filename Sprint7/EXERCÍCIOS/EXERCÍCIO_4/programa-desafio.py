import pandas as pd
import boto3
from datetime import datetime

# Leitura dos arquivos CSV
filmes_df = pd.read_csv('movies.csv', sep ='|')
series_df = pd.read_csv('series.csv', sep ='|')



# Configurações AWS de: KESLEY-WILIE-COSTA-SANTOS
aws_access_key_id ='ASIA5FZDYDWQLVWLHROV'
aws_secret_access_key = '3J1ni89sKiP1+s/J6Tpf2QwwJX/eSSOUXdLY7005'
aws_session_token = 'IQoJb3JpZ2luX2VjEKL//////////wEaCXVzLWVhc3QtMSJHMEUCIQDlhojT+XTUXte0t0EdcwsSvH+M18lyYC6Pz7l0GFu07gIgVbd0tTu/7MOyFB2buUKELJFJgt+5sixGDv/l0zJul/AqnwMIGxAAGgw5MDU3NzYyMDkzMTIiDHNXZaFLS7I/wi3hDCr8AqL2PVAqoBxNt+ni1uobHqruKk6vuEeOLmvZDDXHoGMEcDyXZnNJttyWvwRtJ/n1CjB065FIwV/kIuZVSFpRJW1bDVF5E2zDQb2PoMY/OdihPho400V5EZFxBjYqr/l1eYNpIbC0EwYCZoA7hsljJTJ17EwfZ4p5X4gKIuh5I+hE2oAFtGrA1W2cU2emmaiatzCeSrLrZXWi66VgW0Nr5881pfIiUYxoeMqJkQwFqfVBEPi5nhc+Y6ZNiACdbL1BZE+7eEIXgHl1/O9wP67EhrTq3CJgVRyFl5PaILKiHM5urAc0ssZngldqLrcFEUxb6d8ZL9nfhAzsy/SSSwr6DosZxmMdCZWs19S5Q/6BZGuqgjb300QpaEYKXlRaD77P4+dAKwAhf35oUqqv76hXIxiFojJYqbUlb3hb7a/H7xynrQzHE3i4Asqq+5GvM4LbGdQb/LE+1YkXl2WlGFouRNP1fbHiTifdn8WFkhgfhAkzJuy8hNmFjIIObqP6MNaX3asGOqYBOrfE0/ZRjQlUqPe4s3l5ZPdaypThv8rsUMZHbM2aaw7ruu+lmJPNtGan6+bL3fM8GYR8TEBV5qKtAs9cIo7c6xVgWa6SjYldedwm9zl/chse7azYF67c77G1TTbcVbEH0YWZUZE5IYHQHZ9312W0bBumJ8C4WW0UoHo7Ngpm/GeTg671elpV7N6P9POHXCAYEmOogRscOQDEG5e5Ywclk3hkiW1Rdg=='

regiao = 'us-east-1'
bucket_name = 'data-lake-kesley'
zona_bruta = 'Raw'
local = 'Local'
formato_csv = 'CSV'
pasta_filmes = 'Movies'
pasta_series = 'Series'

session = boto3.Session(aws_access_key_id="ASIA5FZDYDWQJYZUMMVT",
                        aws_secret_access_key="nES3g++VhREscqp64jWN20PoTZlj97TypuWjtoi8",
                        aws_session_token="IQoJb3JpZ2luX2VjEKP//////////wEaCXVzLWVhc3QtMSJHMEUCIE2+MnbQcm4Ktgt/XwfoKglnBBNGmM/HZ1zq2H1WM6+2AiEA6CQ+CNuA2xN1uIEGFKvu3iqM1mvg47se+YnR+OosPNwqnwMIGxAAGgw5MDU3NzYyMDkzMTIiDBHcJfn9wDCWis3PRyr8Aubq3YYy+5g0pibajalgyCbIIDL+waVveUMM4NUmAGntOgTAWxjRkyGGJsZ42pKrC9zi0p+s2ty4qnDLB4OS2Ps+qTND3CxN5vi5UWd/TteBiR9K9tAeOGGi0Gl9WLi6HBB7pA1fsyTKWPeXPUUTOP/WivBa4Z9cGBTLfcq81FpP6PjR2Tn/SmiJYf7hbmZ7CH05TVtA53AgNRFXTd1GDgckbZTcc9vKUdewbeqEnw0f0wcIIH7D33fqwifrgn6NBzjYeh8A/O0feYHaT0F0/u8xMUjJTFJFrF9A6O8tCOqQVMChvre7b9szWn2lUiNKzqP8+LziR0+lCv56LdZVueig2Gjfv+t6vgDC+MIPUk0UTL8Geke0er7WJD7HrrxOaNICNfrjphacwvpvcW/F1CADiHS3RH1Qsxp0wixGWmA11hEbOhJpdCXbKQjJBO9+aqDD+Z0AnYqIWfj5JQL0mGAUONOOvJw6Yw8TEOLqzJuCuJjGxFjzt1oVPl64MJyf3asGOqYBel2rnq/BeAD00G2ZF/z0TekEia76cA2WM4tR8UUJZ/Iz1hmI+J6ZerHidE0dyHeot5/8QrzqeLcpImNs3r1LD7uXNs14cM9Gsr+iC2uVAb93iK3ZnrU4kSyaASKr3nDHscFznRk4p6wRiZ9cilkHSJ4rQJi62E9s/h4WJdBZIAZMYru/SJiVwnJMYhqkPwoxHnR2NuiinX4WNP22JnttJO40oOB3mw==", region_name='us-east-1')
s3 = session.client('s3', region_name='us-east-1', aws_access_key_id="ASIA5FZDYDWQJYZUMMVT",
                    aws_secret_access_key="nES3g++VhREscqp64jWN20PoTZlj97TypuWjtoi8",
                    aws_session_token="IQoJb3JpZ2luX2VjEKP//////////wEaCXVzLWVhc3QtMSJHMEUCIE2+MnbQcm4Ktgt/XwfoKglnBBNGmM/HZ1zq2H1WM6+2AiEA6CQ+CNuA2xN1uIEGFKvu3iqM1mvg47se+YnR+OosPNwqnwMIGxAAGgw5MDU3NzYyMDkzMTIiDBHcJfn9wDCWis3PRyr8Aubq3YYy+5g0pibajalgyCbIIDL+waVveUMM4NUmAGntOgTAWxjRkyGGJsZ42pKrC9zi0p+s2ty4qnDLB4OS2Ps+qTND3CxN5vi5UWd/TteBiR9K9tAeOGGi0Gl9WLi6HBB7pA1fsyTKWPeXPUUTOP/WivBa4Z9cGBTLfcq81FpP6PjR2Tn/SmiJYf7hbmZ7CH05TVtA53AgNRFXTd1GDgckbZTcc9vKUdewbeqEnw0f0wcIIH7D33fqwifrgn6NBzjYeh8A/O0feYHaT0F0/u8xMUjJTFJFrF9A6O8tCOqQVMChvre7b9szWn2lUiNKzqP8+LziR0+lCv56LdZVueig2Gjfv+t6vgDC+MIPUk0UTL8Geke0er7WJD7HrrxOaNICNfrjphacwvpvcW/F1CADiHS3RH1Qsxp0wixGWmA11hEbOhJpdCXbKQjJBO9+aqDD+Z0AnYqIWfj5JQL0mGAUONOOvJw6Yw8TEOLqzJuCuJjGxFjzt1oVPl64MJyf3asGOqYBel2rnq/BeAD00G2ZF/z0TekEia76cA2WM4tR8UUJZ/Iz1hmI+J6ZerHidE0dyHeot5/8QrzqeLcpImNs3r1LD7uXNs14cM9Gsr+iC2uVAb93iK3ZnrU4kSyaASKr3nDHscFznRk4p6wRiZ9cilkHSJ4rQJi62E9s/h4WJdBZIAZMYru/SJiVwnJMYhqkPwoxHnR2NuiinX4WNP22JnttJO40oOB3mw==")

# Data de processamento
data_processamento = datetime.now().strftime('%Y/%m/%d')

# Caminho no S3
filmes_s3_path = f"S3://{bucket_name}/{zona_bruta}/{local}/{formato_csv}/{pasta_filmes}/{data_processamento}/filmes.csv"
series_s3_path = f"S3://{bucket_name}/{zona_bruta}/{local}/{formato_csv}/{pasta_series}/{data_processamento}/series.csv"

# Configuração do cliente S3
#s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token, region_name=regiao)

# Upload para o S3
s3.put_object(Body=filmes_df.to_csv(index=False), Bucket=bucket_name, Key=f'{zona_bruta}/{local}/{formato_csv}/{pasta_filmes}/{data_processamento}/filmes.csv')
s3.put_object(Body=series_df.to_csv(index=False), Bucket=bucket_name, Key=f'{zona_bruta}/{local}/{formato_csv}/{pasta_series}/{data_processamento}/series.csv')