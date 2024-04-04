namespace API.classesEspelho;

public class Init
{
    public static int SUCESSO = 1;
    public static int FALHA = 0;
    public static int SAIDA_FALHA = 1;
    public static int SAIDA_NORMAL = 0;
    public static int ESTADO_INICIAL = 0;
    public static int numTentativas;
    public static int postMaximo;
    public static int requestPacote;
    public static int timeoutSql;
    public static TimeSpan esperaPacote;
    public static TimeSpan esperaApi;
    public static TimeSpan pausa;
    public static TimeSpan timeoutClient;
    public static int threadMaxTask;
    public static int manualOverride;
    public static int manualOverrideEnd;
    public static int manualOverPage;
    public static int threadMaxProc;
    public static int endPage;
    public static string dtFim;
    public static int tipoInsert;
    public static IConfiguration Configuration { get; set; }

    public static void InitializeConfiguration()
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("config.json", optional: true, reloadOnChange: true);

        Configuration = builder.Build();
        manualOverrideEnd = int.Parse(Configuration["manualOverrideEnd"] ?? string.Empty);
        manualOverPage = int.Parse(Configuration["manualOverPage"] ?? string.Empty);
        endPage = int.Parse(Configuration["endPage"] ?? string.Empty);
        manualOverride = int.Parse(Configuration["manualOverride"] ?? string.Empty);
        threadMaxTask = int.Parse(Configuration["threadMaxTask"] ?? string.Empty);
        threadMaxProc = int.Parse(Configuration["threadMaxProc"] ?? string.Empty);
        numTentativas = int.Parse(Configuration["numTentativas"] ?? string.Empty);
        postMaximo = int.Parse(Configuration["postMaximo"] ?? string.Empty);
        requestPacote = int.Parse(Configuration["requestPacote"] ?? string.Empty);
        timeoutSql = int.Parse(Configuration["timeoutSql"] ?? string.Empty);
        tipoInsert = int.Parse(Configuration["tipoInsert"] ?? string.Empty);
        esperaPacote = TimeSpan.FromMilliseconds(double.Parse(Configuration["esperaPacote"] ?? string.Empty));
        esperaApi = TimeSpan.FromMilliseconds(double.Parse(Configuration["esperaApi"] ?? string.Empty));
        pausa = TimeSpan.FromMilliseconds(double.Parse(Configuration["pausa"] ?? string.Empty));
        timeoutClient = TimeSpan.FromMilliseconds(double.Parse(Configuration["timeoutClient"] ?? string.Empty));
    }
}