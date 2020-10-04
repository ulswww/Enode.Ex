using System;

namespace Enode.Ex.Dapper.Impl
{
    public class SqlDialect : ISqlDialect
    {
        private  string _leftlimitedIdentifer;
        private  string _reightlimitedIdentifer;

        public SqlDialect(string leftDelimitedIdentifier,string rightDelimitedIdentifier)
        {
            _leftlimitedIdentifer = leftDelimitedIdentifier;
            _reightlimitedIdentifer = rightDelimitedIdentifier;

            if(string.IsNullOrEmpty(leftDelimitedIdentifier))
            {
                throw new ArgumentNullException(nameof(leftDelimitedIdentifier));
            }
            
            if(string.IsNullOrEmpty(rightDelimitedIdentifier))
            {
                throw new ArgumentNullException(nameof(rightDelimitedIdentifier));
            }
        }
        public SqlDialect()
        {
            _leftlimitedIdentifer = string.Empty;
            _reightlimitedIdentifer = string.Empty;
        }

        public string GetDelimited(string objectName)
        {
            if (_leftlimitedIdentifer==string.Empty || objectName.StartsWith(_leftlimitedIdentifer))
                return objectName;

            return $"{_leftlimitedIdentifer}{objectName}{_reightlimitedIdentifer}";
        }
    }
}