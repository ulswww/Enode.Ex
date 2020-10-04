namespace Enode.Ex.Dapper
{
    public interface ISqlDialect
    {
         string GetDelimited(string objectName);
    }
}