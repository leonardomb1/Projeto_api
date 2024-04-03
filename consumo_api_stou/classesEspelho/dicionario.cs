using Newtonsoft.Json;

namespace API.classesEspelho
{
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
