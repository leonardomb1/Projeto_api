using Newtonsoft.Json;

namespace API.classesEspelho
{
    /// <summary>
    /// Classe que mapeia os retornos possíveis JSON em colunas SQL, ao ler um arquivo mapping.json
    /// </summary>
    public static class DatabaseColumns
    {
        private static readonly Dictionary<string, string> ColumnMapping;

        static DatabaseColumns()
        {
            string mapeamentoColunas = "mapping.json";
            ColumnMapping = LoadMappingFromJson(mapeamentoColunas);
        }

        private static Dictionary<string, string> LoadMappingFromJson(string mapeamentoColunas)
        {
            try
            {
                string jsonContent = File.ReadAllText(mapeamentoColunas);
                return JsonConvert.DeserializeObject<Dictionary<string, string>>(jsonContent);
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine($"Erro: JSON '{mapeamentoColunas}' nao encontrado.");
                return [];
            }
        }
        /// <summary>
        /// Para cada entrada do JSON deserializado a uma lista em memória.
        /// </summary>
        /// <param name="entry">Objeto de entrada do Objeto em mémoria que contém o JSON.</param>
        /// <param name="page">Paginação do retorno</param>
        /// <returns></returns>
        public static Dictionary<string, object> GetSTOU_INSERT(Item entry, int page)
        {
            var mapping = new Dictionary<string, object>();
            foreach (var column in ColumnMapping)
            {
                if (entry.GetType().GetProperty(column.Value) != null)
                {
                    mapping[column.Key] = entry.GetType().GetProperty(column.Value)?.GetValue(entry);
                }
                else
                {
                    mapping[column.Key] = null;
                }
            }
            mapping["page"] = page;
            return mapping;
        }
    }
}
