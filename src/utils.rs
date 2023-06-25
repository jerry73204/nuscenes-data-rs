use crate::serializable::Token;

pub(crate) trait WithToken {
    fn token(&self) -> Token;
}
