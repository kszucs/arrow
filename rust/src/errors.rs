use std::error;


// consider using error_chain or failure, probably the latter


#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
enum StatusCode {
    OK = 0,
    OutOfMemory = 1,
    KeyError = 2,
    TypeError = 3,
    Invalid = 4,
    IOError = 5,
    UnknownError = 9,
    NotImplemented = 10,
    SerializationError = 11,
    PythonError = 12,
    PlasmaObjectExists = 20,
    PlasmaObjectNonexistent = 21,
    PlasmaStoreFull = 22
}
// TODO newtype for using std errors


#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct ArrowError {
    code: StatusCode,
    msg: String
}


impl error::Error for ArrowError {

    fn description(&self) -> &str {
        use self::StatusCode::*;
        match self.code {
            OK => "OK",
            OutOfMemory => "Out of memory",
            KeyError => "Key error",
            TypeError => "Type error",
            Invalid => "Invalid",
            IOError => "IOError",
            UnknownError => "Unknown error",
            NotImplemented => "NotImplemented",
            _ => "Unknown"
        }
    }

}
//TODO: implement conversion traits from std


impl fmt::Display for ArrowError {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }

}


// alias, convention
pub type Error = ArrowError;
