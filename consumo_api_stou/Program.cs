using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.SqlClient;
using API.classesEspelho;
using System.Collections.Concurrent;
using System.Data;
using System.Collections.Immutable;
class Program
{
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
        string connectionString = @"Data Source = INDSNSQL01\DESENVOLVIMENTO; Initial Catalog = bi_rip_rawZone; Integrated Security = True;TrustServerCertificate=True;Encrypt=False;Pooling=True;Max Pool Size=500;MultipleActiveResultSets=True;Connect Timeout=500;";
        string firstDate = "01/01/2024";
        string tabelaDestino = "STOU_JSON_DATA_ponto_espelho";
        string initString = "TWm3CAdbUxZq";
        string restUri = "https://awstou.ifractal.com.br/ripbr/rest/";

        string token = initString + Init.dtFim;
        string sha256Token = ComputeSha256Hash(token);

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

    static ConcurrentDictionary<int, byte> processingPages = new();

    public static async Task<Root> GetDataFromApi(int page, string firstDate, string restUri, string sha256Token)
    {
        Root? recebido = new();

        int i = 0;

        for (; i <= Init.numTentativas; i++)
        {

            try
            {
                HttpResponseMessage response = new();
                using (
                var request = new HttpRequestMessage(HttpMethod.Post, restUri)) 
                {
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
                    response = await client.SendAsync(request);
                }
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

    public static async Task<int> ChamaApi(string connectionString, int maxPages, string firstDate, string tabelaDestino, string restUri, string sha256Token)
    {
        int pagina = (Init.manualOverride == 1) ? Init.manualOverPage : 1;
        int final = (Init.manualOverrideEnd == 1) ? Init.endPage : maxPages;
        int envios = 0;
        var options = new ParallelOptions { MaxDegreeOfParallelism = Init.threadMaxProc };
        var semaphore = new SemaphoreSlim(Init.threadMaxTask);

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

            try
            {
                var results = await Task.WhenAll(tasks);
                if (Init.tipoInsert == 1)
                {
                    await Parallel.ForEachAsync(results, options, async (result, ct) =>
                    {
                        using SqlConnection connection = new(connectionString);
                        await connection.OpenAsync(ct);
                        var page = result.page;
                        var insertTasks = result.itens.Select(entrada => InsertRawAsync(page, connection, entrada, tabelaDestino));
                        await Task.WhenAll(insertTasks);
                    });
                }
                else if (Init.tipoInsert == 2)
                {
                    await Parallel.ForEachAsync(results, options, async (result, ct) => {
                        using SqlConnection connection = new(connectionString);
                        await connection.OpenAsync(ct);
                        List<Item> bulkData = result.itens.Where(x => x != null).Cast<Item>().ToList();
                        await BulkInsertRawAsync(connection, bulkData, tabelaDestino, pagina);
                    });
                }
                else
                {
                    throw new ArgumentException($"Necessario especificar tipo de operacao para execucao! ({nameof(Init.tipoInsert)})");
                }
                
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro: {ex.Message}");
            }
            finally
            {
                semaphore.Release();
                tasks.Clear();
            }
            await Task.Delay(Init.esperaPacote);
        }
        return 1;
    }

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

    public static async Task InsertRawAsync(int page, SqlConnection con, Item entry, string tabelaDestino)
    {
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
                break;
            }

            catch (Exception)
            {
                await Task.Delay(Init.pausa);
            }
        }
    }

    public static async Task BulkInsertRawAsync(SqlConnection con, List<Item> entries, string tabelaDestino, int pages)
    {
        // Create a DataTable to hold the data
        DataTable dt = new DataTable();
        List<string> getColumns = DatabaseColumns.GetSTOU_INSERT(entries.FirstOrDefault(), 1).Keys.ToList();

        // Assume that DatabaseColumns.ColumnMapping contains the column mappings  
        foreach (var column in getColumns)
        {
            dt.Columns.Add(column);
        }

        // Add the data to the DataTable
        foreach (var entry in entries)
        {
            var STOU_INSERT = DatabaseColumns.GetSTOU_INSERT(entry, pages - 1);
            dt.Rows.Add(STOU_INSERT.Values.ToArray());
        }

        // Perform the bulk insert
        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con))
        {
            bulkCopy.DestinationTableName = tabelaDestino;
            await bulkCopy.WriteToServerAsync(dt);
        }
    }
}