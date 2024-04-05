namespace API.classesEspelho;
/// <summary>
/// Classe que contém estrutura aceita para receber um retorno JSON, com uma lista interna de JSONs itens, e um contador do numero total de itens.
/// </summary>
public class Root
{
    public List<Item?>? itens { get; set; }
    public int? totalCount { get; set; }
    public int page {get; set;}
}