using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.SqlClient;
using API.classesEspelho;
using System.Collections.Concurrent;
using System.Data;

/// Leonardo Machado Baptista - 04/04/2024  ///
/// Este fonte realiza extração por meio de uma API REST paginada para uma tabela especificada do SQL, 
/// Enviando um corpo em JSON com uma chave encriptada em SHA256, passando uma data de corte.
/// 
/// Possui-se dois arquivos JSON que são lidos para finalidade de: 
/// 1. Específicar parâmetros para o fonte, quanto a utilização de threads, de tasks, etc.
/// 2. Mapear quais colunas deverão ser enviadas para o SQL
/// **************************************  ///

class Program
{
    /// Realiza-se a inicialização de um objeto criador de requisições HTTP e inicializa-se variáveis e parâmetros 
    /// que são carregados do arquivo de configuração, estes vindos do arquivo namespace API.classesEspelho.

    private static readonly IHttpClientFactory HttpClientFactory;
    public static HttpClient client;
    static Program()
    {
        Init.InitializeConfiguration();

        var services = new ServiceCollection();
        services.AddHttpClient();
        var serviceProvider = services.BuildServiceProvider();
        HttpClientFactory = serviceProvider.GetService<IHttpClientFactory>();

        client = HttpClientFactory.CreateClient();
        client.Timeout = Init.timeoutClient;

        Init.dtFim = DateTime.Now.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("pt-BR"));
    }

    public static async Task<int> Main(string[] args)
    {
        string connectionString = args[0];
        string firstDate = args[1];
        string tabelaDestino = args[2];
        string initString = args[3];
        string restUri = args[4];

        string token = initString + Init.dtFim;
        string sha256Token = ComputeSha256Hash(token);
        
        // Realiza-se primeira chamada à API para buscar número de páginas totais no período filtrado.
        Root firstReturn = await GetDataFromApi(1, firstDate, restUri, sha256Token);
        int maxPages = firstReturn.totalCount ?? 0;

        Console.WriteLine($"Processando Paginas : {maxPages}");
        int exec = await ChamaApi(connectionString, maxPages, firstDate, tabelaDestino, restUri, sha256Token);

        if (exec == 1)
        {
            Console.WriteLine("Finalizado.");
            return 0;
        }

        else
        { 
            return 1;
        }
    }

    // Inicializa-se contador de paginas concorrentes, para processamento multi-thread.
    static ConcurrentDictionary<int, byte> processingPages = new();

    public static async Task<Root> GetDataFromApi(int page, string firstDate, string restUri, string sha256Token)
    {
        Root? recebido = new();

        int i = 0;

        for (; i <= Init.numTentativas; i++)
        {
            // TODO: No momento este programa realiza a chamada a um WS fixo, declarado dentro do fonte. É necessário alterar
            // para que seja possível passar um Body diferente para o Json, nos casos de outras utilizações.
            try
            {
                var request = new HttpRequestMessage(HttpMethod.Get, restUri);
                request.Headers.Clear();
                request.Headers.Add("user", "integracao");
                request.Headers.Add("token", sha256Token);
                request.Content = new StringContent
                ($@"{{
                        ""pag"" : ""ponto_espelho"",
                        ""cmd"" : ""get"",
                        ""dtde"" : ""{firstDate}"",
                        ""dtate"" : ""{Init.dtFim}"",
                        ""start"":""1"",
                        ""page"":""{page}""
                    }}", Encoding.UTF8, "application/json");
                using var response = await client.SendAsync(request);
                recebido = await response.Content.ReadAsAsync<Root>();
                recebido.page = page;
                break;
            }
            catch (Exception ex) when (i < Init.numTentativas - 1)
            {
                Console.WriteLine($"Erro aconteceu na pagina : {page} - Com erro {ex.Message} Aguardando {Init.pausa} segundos.");
                await Task.Delay(Init.pausa);
            }
        }

        if (i > Init.numTentativas) { Console.WriteLine($"Falha ao extrair pacote na pagina: {page}"); }
        return recebido;
    }
    /// <summary>
    /// Este método é responsável por coordenar a chamada à API e inserir ao Banco de dados. Chamando outros métodos que contém a lógica necessária.
    /// </summary>
    /// <param name="connectionString">String para conexão</param>
    /// <param name="maxPages">Número máximo de páginas</param>
    /// <param name="firstDate">Data de corte a ser considerada</param>
    /// <param name="tabelaDestino">Tabela de destino da inserção</param>
    /// <param name="restUri"></param>
    /// <param name="sha256Token"></param>
    /// <returns>Retornará 1 em Sucesso da operação de extração e 0 para falha da operação</returns>
    /// <exception cref="ArgumentException"></exception>
    public static async Task<int> ChamaApi(string connectionString, int maxPages, string firstDate, string tabelaDestino, string restUri, string sha256Token)
    {
        // Inicializa-se os parâmetros configurados nos arquivos JSON.
        int pagina = (Init.manualOverride == 1) ? Init.manualOverPage : 1;
        int final = (Init.manualOverrideEnd == 1) ? Init.endPage : maxPages;
        int envios = 0;
        int exec = Init.ESTADO_INICIAL;
        var options = new ParallelOptions { MaxDegreeOfParallelism = Init.threadMaxProc };
        var semaphore = new SemaphoreSlim(Init.threadMaxTask);

        // Abaixo é realizado GET em várias threads em simultaneo, sendo rateados as páginas utilizando um "semáforo" de Tasks.
        for (int i = 0; pagina <= final; i++)
        {
            var tasks = new List<Task<Root>>();

            for (int j = 0; j <= Init.requestPacote; j++)
            {
                if (pagina > maxPages) { break; }
                if (!processingPages.TryAdd(pagina, 0)) { continue; }

                await semaphore.WaitAsync();
                tasks.Add(GetDataFromApi(pagina, firstDate, restUri, sha256Token).ContinueWith(t =>
                {
                    semaphore.Release();
                    return t.Result;
                }));

                Console.WriteLine($"Carregando pagina: {pagina}");
                envios++;
                if (envios > Init.postMaximo) { Console.WriteLine("..."); await Task.Delay(Init.esperaApi); envios = 0; }
                pagina++;
            }
            Console.WriteLine($"Processando paginas {pagina - Init.requestPacote - 1} - {pagina - 1}");
            
            // Abaixo é realizado a inserção das páginas capturadas ao SQL, realizando inserção de acordo com o parâmetro tipoInsert
            // configurado, que é utilizado para definir o método de inserção, sendo ele em massa ou linha a linha.
            try
            {
                var results = await Task.WhenAll(tasks);
                if (Init.tipoInsert == 1)
                {
                    exec = await RealizaInsertRaw(tabelaDestino, connectionString, options, results);
                    
                }

                // Se não houver sucesso de inserção em massa utilizar inserção em linha.
                else if (Init.tipoInsert == 2)
                {
                    exec = await RealizaInsertBulkInsert(pagina, tabelaDestino, connectionString, options, results);
                    
                    if (exec != Init.SUCESSO) await RealizaInsertRaw(tabelaDestino, connectionString, options, results);
                }

                else
                {
                    throw new ArgumentException($"Necessario especificar tipo de operacao (1 ou 2) para execucao! ({nameof(Init.tipoInsert)})");
                }
                
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro: {ex.Message}");
            }
            // Liberando mémoria das threads abertas.
            finally
            {
                semaphore.Release();
                tasks.Clear();
            }
            await Task.Delay(Init.esperaPacote);
        }
        return exec;
    }

    // Este método realiza a criptografia em SHA256, padrão para envio de token à API.
    public static string ComputeSha256Hash(string rawData)
    {
        using SHA256 sha256Hash = SHA256.Create();
        byte[] bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(rawData));
        StringBuilder builder = new();
        for (int i = 0; i < bytes.Length; i++)
        {
            builder.Append(bytes[i].ToString("x2"));
        }
        return builder.ToString();
    }

    /// <summary>
    /// Este método realiza chamada ao método de inserção em linha e o executa em um for each paralelo em várias threads,
    /// Cada thread inserirá uma linha de alguma página no banco.
    /// </summary>
    /// <param name="tabelaDestino">Tabela de destino da inserção</param>
    /// <param name="connectionString">String para conexão</param>
    /// <param name="options">Opção de configuração de paralelismo de thread.</param>
    /// <param name="results">Objeto que contém o resultado da extração.</param>
    /// <returns>Retornará 1 em sucesso da operação e 0 em caso de falha na operação.</returns>
    public static async Task<int> RealizaInsertRaw(string tabelaDestino, string connectionString, ParallelOptions options, Root[]? results)
    {
        await Parallel.ForEachAsync(results, options, async (result, ct) =>
        {
            using SqlConnection connection = new(connectionString);
            await connection.OpenAsync(ct);

            var page = result.page;
            var insertTasks = result.itens.Select(entrada => InsertRawAsync(page, connection, entrada, tabelaDestino));

            await Task.WhenAll(insertTasks);
        });
        return Init.SUCESSO;
    }
    /// <summary>
    /// Este método realiza chamada ao método de inserção em massa e o executa em um for each paralelo em várias threads,
    /// Cada thread inserirá uma página inteira ao banco.
    /// </summary>
    /// <param name="pagina">Numero de páginas do que foram iteradas.</param>
    /// <param name="tabelaDestino">Tabela de destino da inserção.</param>
    /// <param name="connectionString">String para conexão.</param>
    /// <param name="options">Opção de configuração de paralelismo de thread.</param>
    /// <param name="results">Objeto que contém o resultado da extração em um array de objetos.</param>
    /// <returns>Retornará 1 em sucesso da operação e 0 em caso de falha na operação.</returns>
    public static async Task<int> RealizaInsertBulkInsert(int pagina, string tabelaDestino, string connectionString,ParallelOptions options, Root[]? results)
    {
        int exec = Init.ESTADO_INICIAL;
        using SqlConnection connection = new(connectionString);
        await connection.OpenAsync();

        foreach (var result in results)
        {
            var bulkData = result.itens.Where(x => x != null).Cast<Item>().ToList();
            exec = await BulkInsertRawAsync(connection, bulkData, tabelaDestino, pagina);
        }

        return exec;
    }
    /// <summary>
    /// Este método contém a lógica de inserção linha a linha ao banco de dados, realizando-a mapeando cada chave do arquivo de configuração à uma
    /// coluna em uma instrução de inserção dinâmica.
    /// </summary>
    /// <param name="page">Numero da página da pessoa.</param>
    /// <param name="con">Objeto que contém conexão ao banco de dados.</param>
    /// <param name="entry">Objeto que contém o resultado da extração.</param>
    /// <param name="tabelaDestino">Tabela de destino da inserção.</param>
    /// <returns>Retornará 1 em sucesso da operação e 0 em caso de falha na operação.</returns>
    public static async Task<int> InsertRawAsync(int page, SqlConnection con, Item entry, string tabelaDestino)
    {
        int exec = Init.ESTADO_INICIAL;

        for (int i = 0; i < 5; i++)
        {
            try
            {
                var STOU_INSERT = DatabaseColumns.GetSTOU_INSERT(entry, page);

                string columns = string.Join(", ", STOU_INSERT.Keys);
                string parameters = string.Join(", ", STOU_INSERT.Keys.Select((_, i) => $"@Value{i + 1}"));

                string insertStatement = $"INSERT INTO {tabelaDestino} ({columns}) VALUES ({parameters})";
                using SqlCommand command = new(insertStatement, con)
                {
                    CommandTimeout = Init.timeoutSql
                };

                for (int j = 0; j < STOU_INSERT.Count; j++)
                {
                    var value = STOU_INSERT.Values.ElementAt(j);
                    command.Parameters.AddWithValue($"@Value{j + 1}", value ?? DBNull.Value);
                }

                int rowsAffected = await command.ExecuteNonQueryAsync();
                exec = Init.SUCESSO;
                break;
            }

            catch (Exception ex)
            {
                File.WriteAllText($"log{DateTime.UtcNow}_PG_{page}.txt", ex.ToString());
                await Task.Delay(Init.pausa);
            }
        }

        return exec;
    }
    /// <summary>
    /// Este método contém a lógica de inserção em massa ao banco de dados, realizando-a mapeando cada chave do arquivo de configuração à uma 
    /// coluna do banco de dados, criando uma tabela em mémoria e transferindo-a em uma instrução de Bulk Insert ao banco de dados.
    /// </summary>
    /// <param name="con">Objeto que contém conexão ao banco de dados.</param>
    /// <param name="entries">Objeto que contém o resultado da extração em um array de objetos.</param>
    /// <param name="tabelaDestino">Tabela de destino da inserção.</param>
    /// <param name="pages">Numero de páginas do que foram iteradas.</param>
    /// <returns>Retornará 1 em sucesso da operação e 0 em caso de falha na operação.</returns>
    public static async Task<int> BulkInsertRawAsync(SqlConnection con, List<Item> entries, string tabelaDestino, int pages)
    {
        int exec = Init.ESTADO_INICIAL;

        for (int i = 0; i < 5; i++)
        {
            try
            {
                DataTable dt = new DataTable();
                List<string> getColumns = DatabaseColumns.GetSTOU_INSERT(entries.FirstOrDefault(), 1).Keys.ToList(); 
                
                foreach (var column in getColumns)
                {
                    dt.Columns.Add(column);
                }

                foreach (var entry in entries)
                {
                    var STOU_INSERT = DatabaseColumns.GetSTOU_INSERT(entry, pages - 1);
                    dt.Rows.Add(STOU_INSERT.Values.ToArray());
                }

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con))
                {
                    bulkCopy.DestinationTableName = tabelaDestino;
                    await bulkCopy.WriteToServerAsync(dt);
                }

                exec = Init.SUCESSO;
                break;
            }
            catch (Exception ex)
            {
                File.WriteAllText($"log_{DateTime.UtcNow:dd_mm_yyyy_HH_mm_ss}_PG_{pages}.txt", ex.ToString());
                await Task.Delay(Init.pausa);
            }
        }

        return exec;
    }
}