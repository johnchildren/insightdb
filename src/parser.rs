use chrono::prelude::*;
use engine::{Expr, Query, BinOp, UnrOp, Filter, Predicate};

#[derive(Debug, PartialEq)]
enum Token {
    Comma,
    Colon,
    Select,
    Id(String),
    LParen,
    RParen,
    By,
    From,
    Where,
    EOF,
    Underscore,
    Int(i32),
    Add,
    Sub,
    Sum,
    Sums,
    Max,
    Maxs,
    Min,
    Mins,
    Product,
    Products,
    Mul,
    Div,
    StrLit(String),
    Range,
    Dot,
    TimeSep,
    Til,
    Less,
    Equal,
    Greater,
}

#[derive(Debug)]
struct Scanner {
    cmd: Vec<char>,
    pos: usize,
    tok: Token,
}

impl Scanner {
    #[inline]
    fn new(s: &str) -> Self {
        Scanner {
            cmd: s.chars().collect(),
            pos: 0,
            tok: Token::Underscore,
        }
    }

    #[inline]
    fn next_token(&mut self) -> Result<Token, &'static str> {
        let token = match self.cur_char() {
            Some(' ') => return self.scan_whitespace(),
            Some('a'..'z') => return self.scan_id_or_keyword(),
            Some('T') => Token::TimeSep,
            Some('(') => Token::LParen,
            Some(')') => Token::RParen,
            Some(',') => Token::Comma,
            Some('0'..'9') => return self.scan_number(),
            Some('+') => Token::Add,
            Some('-') => Token::Sub,
            Some('*') => Token::Mul,
            Some('/') => Token::Div,   
            Some('"') => return self.scan_str_literal(),  
            Some('\n') => Token::EOF,  
            Some('.') => Token::Dot,    
            Some(':') => Token::Colon, 
            Some('<') => Token::Less,
            Some('>') => Token::Greater,
            Some('=') => Token::Equal,
            Some(c) => {
                println!("unexpected char={:?}", c);
                return Err("unexpected token");
            }
            None => Token::EOF,
        };
        self.pos += 1;
        Ok(token)
    }

    fn scan_str_literal(&mut self) -> Result<Token, &'static str> {
        self.pos += 1;
        let mut lit = String::new();
        loop {
            match self.cur_char() {
                Some('"') => {
                    self.pos += 1;
                    break;
                }
                Some(c) => lit.push(c),
                None => return Err("unclosed string literal"),
            }
            self.pos += 1;
        }
        Ok(Token::StrLit(lit))
    }

    fn scan_number(&mut self) -> Result<Token, &'static str> {
        let mut s = String::new();
        loop {
            match self.cur_char() {
                Some(c @ '0'..'9') => s.push(c),
                Some('+') | Some('-') | Some('*') | Some('/') | Some(' ') | Some(',') |
                Some('(') | Some(')') | Some(':') | Some('.') | Some('T') | Some('<') | Some('=') | Some('>') | None => break,
                Some(c) => {
                    println!("c = {}", c);
                    return Err("unexpected digit");
                }
            }
            self.pos += 1;
        }
        match s.parse::<i32>() {
            Ok(val) => Ok(Token::Int(val)),
            Err(_) => Err("cannot parse int"),
        }
    }

    fn peek_next_token(&mut self) -> Result<Token, &'static str> {
        let pos = self.pos;
        let res = self.next_token();
        self.pos = pos;
        res
    }

    fn scan_whitespace(&mut self) -> Result<Token, &'static str> {
        loop {
            match self.cur_char() {
                Some(' ') => self.pos += 1,
                Some(_) => return self.next_token(),
                None => return Ok(Token::EOF),
            }
        }
    }

    fn scan_id_or_keyword(&mut self) -> Result<Token, &'static str> {
        let mut id = String::new();
        loop {
            match self.cur_char() {
                Some('(') | Some(')') | Some(' ') | Some(',') | Some('+') | Some('-') |
                Some('*') | Some('/') | Some('\n') | Some('<') | Some('=') | Some('>') | None => break,
                Some(c) => id.push(c), 
            }
            self.pos += 1;
        }
        let tok = match id.as_ref() {
            "select" => Token::Select,
            "from" => Token::From,
            "by" => Token::By,
            "where" => Token::Where,
            "sum" => Token::Sum,
            "sums" => Token::Sums,
            "product" => Token::Product,
            "products" => Token::Products,
            "max" => Token::Max,
            "min" => Token::Min,
            "maxs" => Token::Maxs,
            "mins" => Token::Mins,
            "range" => Token::Range,
            "til" => Token::Til,
            _ => Token::Id(id),
        };
        Ok(tok)
    }

    #[inline]
    fn cur_char(&self) -> Option<char> {
        self.cmd.get(self.pos).map(|c| *c)
    }
}

#[derive(Debug)]
pub struct Parser {
    scanner: Scanner,
}

impl Parser {
    pub fn new(s: &str) -> Self {
        Parser { scanner: Scanner::new(s) }
    }

    pub fn parse(&mut self) -> Result<Query, &'static str> {
        let select = self.parse_select()?;
        let by = self.parse_by()?;
        let from = self.parse_from()?;
        let filters = self.parse_where()?;
        Ok(Query::from(select, by, from, filters))
    }

    fn parse_select(&mut self) -> Result<Vec<Expr>, &'static str> {
        match self.next_token() {
            Ok(Token::Select) => (),
            Ok(_) => return Err("expected select token"),
            Err(err) => return Err(err),
        }
        let mut exprs = Vec::new();
        loop {
            match self.parse_expr() {
                Ok(expr) => exprs.push(expr),
                Err(err) => return Err(err),
            }
            match self.peek_next_token() {
                Ok(Token::Comma) => {
                    let _ = self.scanner.next_token().unwrap();
                }
                Err(err) => return Err(err),
                Ok(_) => break,
            }
        }
        Ok(exprs)
    }

    fn parse_unr_fn(&mut self, op: UnrOp) -> Result<Expr, &'static str> {
        match self.next_token() {
            Ok(Token::LParen) => (),
            Ok(_) => return Err("expected lparen"),
            Err(err) => return Err(err),
        }
        let expr = self.parse_expr()?;
        match self.next_token() {
            Ok(Token::RParen) => (),
            Ok(_) => return Err("expected rparen"),
            Err(err) => return Err(err),
        }
        Ok(Expr::UnrFn(op, Box::new(expr)))
    }

    #[inline]
    fn parse_filter(&mut self) -> Result<Filter, &'static str> {
        let lhs = self.parse_expr()?;
        let op = self.parse_predicate()?;
        let rhs = self.parse_expr()?;
        Ok(Filter::new(lhs, op, rhs))
    }

    #[inline]
    fn parse_predicate(&mut self) -> Result<Predicate, &'static str> {
        match self.next_token() {
            Ok(Token::Equal) => {
                match self.next_token() {
                    Ok(Token::Equal) => Ok(Predicate::Equal),
                    Ok(_) => Err("unexpected token"),
                    Err(err) => Err(err),
                }
            }
            Ok(Token::Less) => {
                match self.peek_next_token() {
                    Ok(Token::Equal) => {
                        let _ = self.next_token()?;
                        Ok(Predicate::LessEqual)
                    }
                    Ok(_) => Ok(Predicate::Less),
                    Err(err) => Err(err),
                }
            }
            Ok(Token::Greater) => {
                 match self.peek_next_token() {
                    Ok(Token::Equal) => {
                        let _ = self.next_token()?;
                        Ok(Predicate::GreaterEqual)
                    }
                    Ok(_) => Ok(Predicate::Greater),
                    Err(err) => Err(err),
                }
            }
            Ok(_) => Err("unexpected token"),
            Err(err) => Err(err),
        }
    }

    #[inline]
    fn parse_expr(&mut self) -> Result<Expr, &'static str> {
        let lhs = match self.next_token() {
            Ok(Token::Id(id)) => Expr::Id(id),
            Ok(Token::Int(val)) => self.parse_num(val)?,
            Ok(Token::Sum) => {
                match self.parse_unr_fn(UnrOp::Sum) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }
            Ok(Token::Sums) => {
                match self.parse_unr_fn(UnrOp::Sums) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }            
            Ok(Token::Product) => {
                match self.parse_unr_fn(UnrOp::Product) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }
            Ok(Token::Products) => {
                match self.parse_unr_fn(UnrOp::Products) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }            
            Ok(Token::Min) => {
                match self.parse_unr_fn(UnrOp::Min) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }
            Ok(Token::Mins) => {
                match self.parse_unr_fn(UnrOp::Mins) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }                
            Ok(Token::Max) => {
                match self.parse_unr_fn(UnrOp::Max) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }  
            Ok(Token::Maxs) => {
                match self.parse_unr_fn(UnrOp::Maxs) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            } 
            Ok(Token::Til) => {
                match self.parse_unr_fn(UnrOp::Til) {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }                 
            Ok(Token::Range) => {
                match self.parse_range_fn() {
                    Ok(expr) => expr,
                    Err(err) => return Err(err),
                }
            }
            Ok(Token::StrLit(lit)) => Expr::Str(lit),                              
            Ok(_) => unimplemented!(),
            Err(err) => return Err(err),
        };
        //println!("lhs={:?}", lhs);
        let op = match self.peek_next_token() {
            Ok(Token::Add) => BinOp::Add,
            Ok(Token::Sub) => BinOp::Sub,
            Ok(Token::Mul) => BinOp::Mul,
            Ok(Token::Div) => BinOp::Div,            
            Ok(_) => return Ok(lhs),
            Err(err) => return Err(err),
        };
        let _ = self.scanner.next_token().unwrap();
        //println!("op={:?}", op);
        let rhs = match self.parse_expr() {
            Ok(expr) => expr,
            Err(err) => return Err(err),
        };
        //println!("rhs={:?}", rhs);
        Ok(Expr::BinFn(Box::new(lhs), op, Box::new(rhs)))
    }

    fn parse_num(&mut self, val: i32) -> Result<Expr, &'static str> {
        match self.peek_next_token() {
            Ok(Token::Dot) => self.parse_datetime(val),
            Ok(_) => Ok(Expr::Int(val)),
            Err(err) => Err(err),
        }
    }

    fn parse_datetime(&mut self, year: i32) -> Result<Expr, &'static str> {
        match self.next_token() {
            Ok(Token::Dot) => (),
            Ok(_) => return Err("expected dot after year"),
            Err(err) => return Err(err),
        }
        let month = match self.next_token() {
            Ok(Token::Int(val)) => val,
            Ok(_) => return Err("unexpected dot before month"),
            Err(err) => return Err(err),
        };
        match self.next_token() {
            Ok(Token::Dot) => (),
            Ok(_) => return Err("expected dot after month"),
            Err(err) => return Err(err),
        }
        let date = match self.next_token() {
            Ok(Token::Int(val)) => val,
            Ok(_) => return Err("unexpected dot before month"),
            Err(err) => return Err(err),
        };
        match self.next_token() {
            Ok(Token::TimeSep) => (),
            Ok(_) => return Err("expected dot after month"),
            Err(err) => return Err(err),
        } 
        let hour = match self.next_token() {
            Ok(Token::Int(val)) => val as u32,
            Ok(_) => return Err("unexpected dot before month"),
            Err(err) => return Err(err),
        };  
        match self.next_token() {
            Ok(Token::Colon) => (),
            Ok(_) => return Err("expected dot after month"),
            Err(err) => return Err(err),
        }     
        let min = match self.next_token() {
            Ok(Token::Int(val)) => val as u32,
            Ok(_) => return Err("unexpected dot before month"),
            Err(err) => return Err(err),
        }; 
        match self.next_token() {
            Ok(Token::Colon) => (),
            Ok(_) => return Err("expected dot after month"),
            Err(err) => return Err(err),
        }   
        let sec = match self.next_token() {
            Ok(Token::Int(val)) => val as u32,
            Ok(_) => return Err("unexpected dot before month"),
            Err(err) => return Err(err),
        };                                      
        let dt = Utc.ymd(year, month as u32, date as u32).and_hms(hour, min, sec);
        Ok(Expr::DateTime(dt))       
    }    

    fn expect(&mut self, exp: Token) -> Option<&'static str> {
        match self.next_token() {
            Ok(ref tok) if &exp == tok => None,
            Ok(_) => Some("unexpected token"),
            Err(err) => Some(err),
        }
    }

    fn parse_range_fn(&mut self) -> Result<Expr, &'static str> {
        if let Some(err) = self.expect(Token::LParen) {
            return Err(err);
        }
        let lhs = Box::new(self.parse_expr()?);
        match self.peek_next_token() {
            Ok(Token::Comma) => (),
            Ok(_) => return Ok(Expr::UnrFn(UnrOp::Range, lhs)),
            Err(err) => return Err(err),
        }
        let _ = self.next_token().unwrap();
        let rhs = Box::new(self.parse_expr()?);
        Ok(Expr::BinFn(lhs, BinOp::Range, rhs))
    }

    fn parse_from(&mut self) -> Result<Expr, &'static str> {
        match self.next_token() {
            Ok(Token::From) => (),
            Ok(_) => return Err("unexpected tok: expected from"),
            Err(err) => return Err(err),
        }
        match self.parse_expr() {
            Ok(expr) => Ok(expr),
            Err(err) => Err(err),
        }
    }

    fn parse_by(&mut self) -> Result<Option<Vec<Expr>>, &'static str> {
        match self.peek_next_token() {
            Ok(Token::By) => {
                let _ = self.next_token().unwrap();
            }
            Ok(_) => return Ok(None),
            Err(err) => return Err(err),
        }
        let mut exprs = Vec::new();
        loop {
            match self.parse_expr() {
                Ok(expr) => exprs.push(expr),
                Err(err) => return Err(err),
            }
            match self.peek_next_token() {
                Ok(Token::Comma) => {
                    self.next_token().unwrap();
                }
                Ok(Token::EOF) | Ok(Token::From) => break,
                Ok(_) => return Err("unexpected tok in parsing by"),
                Err(err) => return Err(err),
            }
        }
        Ok(Some(exprs))
    }

    #[inline]
    fn peek_next_token(&mut self) -> Result<Token, &'static str> {
        match self.scanner.peek_next_token() {
            Ok(tok) => Ok(tok),
            Err(err) => Err(err),
        }
    }

    #[inline]
    fn parse_where(&mut self) -> Result<Option<Vec<Filter>>, &'static str> {
        match self.peek_next_token() {
            Ok(Token::Where) => {
                let _ = self.next_token().unwrap();
            }
            Ok(_) => return Ok(None),
            Err(_) => return Err("cannot get next token parsing where"),
        }
        let mut filters = Vec::new();
        loop {
            match self.parse_filter() {
                Ok(filter) => filters.push(filter),
                Err(err) => return Err(err),
            }
            match self.peek_next_token() {
                Ok(Token::Comma) => {
                    self.next_token().unwrap();
                }
                Ok(Token::EOF) => break,
                Ok(_) => return Err("unexpected token: expecting EOF"),
                Err(err) => return Err(err),
            }
        }
        Ok(Some(filters))
    }

    #[inline]
    fn next_token(&mut self) -> Result<Token, &'static str> {
        self.scanner.next_token()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

#[test]
fn scan_select_token() {
    let mut scanner = Scanner::new("select");
    assert_eq!(scanner.next_token(), Ok(Token::Select));
    scanner = Scanner::new("  select");
    assert_eq!(scanner.next_token(), Ok(Token::Select));
    scanner = Scanner::new("  select   ");
    assert_eq!(scanner.next_token(), Ok(Token::Select));
}

#[test]
fn scan_from_token() {
    let mut scanner = Scanner::new("from");
    assert_eq!(scanner.next_token(), Ok(Token::From));
}

#[test]
fn scan_int_token() {
    let mut scanner = Scanner::new("1234");
    assert_eq!(scanner.next_token(), Ok(Token::Int(1234)));
}

#[test]
fn scan_by_token() {
    let mut scanner = Scanner::new("by");
    assert_eq!(scanner.next_token(), Ok(Token::By));
}

#[test]
fn scan_where_token() {
    let mut scanner = Scanner::new("where");
    assert_eq!(scanner.next_token(), Ok(Token::Where));
}

#[test]
fn scan_id_token() {
    let mut scanner = Scanner::new("a");
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("a"))));
}

#[test]
fn scan_comma_token() {
    let mut scanner = Scanner::new(",");
    assert_eq!(scanner.next_token(), Ok(Token::Comma));
}

#[test]
fn scan_lparen_token() {
    let mut scanner = Scanner::new("(");
    assert_eq!(scanner.next_token(), Ok(Token::LParen));
}

#[test]
fn scan_rparen_token() {
    let mut scanner = Scanner::new(")");
    assert_eq!(scanner.next_token(), Ok(Token::RParen));
}

#[test]
fn scan_query() {
    let mut scanner = Scanner::new("select a,b from t");
    assert_eq!(scanner.next_token(), Ok(Token::Select));
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("a"))));
    assert_eq!(scanner.next_token(), Ok(Token::Comma));
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("b"))));
    assert_eq!(scanner.next_token(), Ok(Token::From));
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("t"))));
    assert_eq!(scanner.next_token(), Ok(Token::EOF));
}

#[test]
fn scan_range_unr_fn() {
    let mut scanner = Scanner::new("range(a)");
    assert_eq!(scanner.next_token(), Ok(Token::Range));
    assert_eq!(scanner.next_token(), Ok(Token::LParen));
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("a"))));
    assert_eq!(scanner.next_token(), Ok(Token::RParen));
    assert_eq!(scanner.next_token(), Ok(Token::EOF));
}

#[test]
fn scan_range_bin_fn() {
    let mut scanner = Scanner::new("range(1, 10)");
    assert_eq!(scanner.next_token(), Ok(Token::Range));
    assert_eq!(scanner.next_token(), Ok(Token::LParen));
    assert_eq!(scanner.next_token(), Ok(Token::Int(1)));
    assert_eq!(scanner.next_token(), Ok(Token::Comma));
    assert_eq!(scanner.next_token(), Ok(Token::Int(10)));
    assert_eq!(scanner.next_token(), Ok(Token::RParen));
    assert_eq!(scanner.next_token(), Ok(Token::EOF));
}

#[test]
fn parse_select_expr() {
    let mut parser = Parser::new("select a, b");
    let a = Expr::Id(String::from("a"));
    let b = Expr::Id(String::from("b"));
    assert_eq!(parser.parse_select(), Ok(vec![a, b]));
}

#[test]
fn parse_by_expr() {
    let mut parser = Parser::new("by c");
    assert_eq!(
        parser.parse_by(),
        Ok(Some(vec![Expr::Id(String::from("c"))]))
    );
}

#[test]
fn parse_by_none() {
    let mut parser = Parser::new("from t");
    assert_eq!(parser.parse_by(), Ok(None));
}

#[test]
fn parse_where_expr() {
    let mut parser = Parser::new("from t");
    assert_eq!(parser.parse_from(), Ok(Expr::Id(String::from("t"))));
}

#[test]
fn parse_str_literal_expr() {
    let mut parser = Parser::new("\"asc123[]\"");
    assert_eq!(
        parser.parse_expr().unwrap(),
        Expr::Str(String::from("asc123[]"))
    );
}

#[test]
fn parse_query() {
    let mut parser = Parser::new("select b,c by a from t where b==1,c>2");
    let select = vec![Expr::Id(String::from("b")), Expr::Id(String::from("c"))];
    let by = Some(vec![Expr::Id(String::from("a"))]);
    let from = Expr::Id(String::from("t"));
    let filters = Some(vec![
        Filter::new(Expr::Id("b".to_string()), Predicate::Equal, Expr::Int(1)),
        Filter::new(Expr::Id("c".to_string()), Predicate::Greater, Expr::Int(2)),
    ]);
    assert_eq!(
        parser.parse().unwrap(),
        Query::from(select, by, from, filters)
    );
}

#[test]
fn parse_add_strings_expr() {
    let mut parser = Parser::new("\"a1\"+\"b2\"");
    let lhs = Box::new(Expr::Str(String::from("a1")));
    let rhs = Box::new(Expr::Str(String::from("b2")));
    let expr = Expr::BinFn(lhs, BinOp::Add, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_add_cols_expr() {
    let mut parser = Parser::new("a+b");
    let lhs = Box::new(Expr::Id(String::from("a")));
    let rhs = Box::new(Expr::Id(String::from("b")));
    let expr = Expr::BinFn(lhs, BinOp::Add, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_add_col_int_expr() {
    let mut parser = Parser::new("a+123");
    let lhs = Box::new(Expr::Id(String::from("a")));
    let rhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Add, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_add_int_col_expr() {
    let mut parser = Parser::new("123+a");
    let rhs = Box::new(Expr::Id(String::from("a")));
    let lhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Add, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_add_int_int_expr() {
    let mut parser = Parser::new("123+456");
    let rhs = Box::new(Expr::Int(456));
    let lhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Add, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_sub_cols_expr() {
    let mut parser = Parser::new("a-b");
    let lhs = Box::new(Expr::Id(String::from("a")));
    let rhs = Box::new(Expr::Id(String::from("b")));
    let expr = Expr::BinFn(lhs, BinOp::Sub, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_sub_col_int_expr() {
    let mut parser = Parser::new("a-123");
    let lhs = Box::new(Expr::Id(String::from("a")));
    let rhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Sub, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_sub_int_col_expr() {
    let mut parser = Parser::new("123-a");
    let rhs = Box::new(Expr::Id(String::from("a")));
    let lhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Sub, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_sub_int_int_expr() {
    let mut parser = Parser::new("123-456");
    let rhs = Box::new(Expr::Int(456));
    let lhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Sub, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_mul_cols_expr() {
    let mut parser = Parser::new("a*b");
    let lhs = Box::new(Expr::Id(String::from("a")));
    let rhs = Box::new(Expr::Id(String::from("b")));
    let expr = Expr::BinFn(lhs, BinOp::Mul, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_mul_col_int_expr() {
    let mut parser = Parser::new("a*123");
    let lhs = Box::new(Expr::Id(String::from("a")));
    let rhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Mul, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_mul_int_col_expr() {
    let mut parser = Parser::new("123*a");
    let rhs = Box::new(Expr::Id(String::from("a")));
    let lhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Mul, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_mul_int_int_expr() {
    let mut parser = Parser::new("123*456");
    let rhs = Box::new(Expr::Int(456));
    let lhs = Box::new(Expr::Int(123));
    let expr = Expr::BinFn(lhs, BinOp::Mul, rhs);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_sum_expr() {
    let mut parser = Parser::new("sum(a)");
    let arg = Box::new(Expr::Id(String::from("a")));
    let expr = Expr::UnrFn(UnrOp::Sum, arg);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_sums_expr() {
    let mut parser = Parser::new("sums(a)");
    let arg = Box::new(Expr::Id(String::from("a")));
    let expr = Expr::UnrFn(UnrOp::Sums, arg);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_max_expr() {
    let mut parser = Parser::new("max(a)");
    let arg = Box::new(Expr::Id(String::from("a")));
    let expr = Expr::UnrFn(UnrOp::Max, arg);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_maxs_expr() {
    let mut parser = Parser::new("maxs(a)");
    let arg = Box::new(Expr::Id(String::from("a")));
    let expr = Expr::UnrFn(UnrOp::Maxs, arg);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_min_expr() {
    let mut parser = Parser::new("min(a)");
    let arg = Box::new(Expr::Id(String::from("a")));
    let expr = Expr::UnrFn(UnrOp::Min, arg);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_mins_expr() {
    let mut parser = Parser::new("mins(a)");
    let arg = Box::new(Expr::Id(String::from("a")));
    let expr = Expr::UnrFn(UnrOp::Mins, arg);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_product_expr() {
    let mut parser = Parser::new("product(a)");
    let arg = Box::new(Expr::Id(String::from("a")));
    let expr = Expr::UnrFn(UnrOp::Product, arg);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn parse_products_expr() {
    let mut parser = Parser::new("products(a)");
    let arg = Box::new(Expr::Id(String::from("a")));
    let expr = Expr::UnrFn(UnrOp::Products, arg);
    assert_eq!(parser.parse_expr().unwrap(), expr);
}

#[test]
fn scan_datetime() {
    let mut scanner = Scanner::new("2014.03.01T00:23:02");
    assert_eq!(scanner.next_token(), Ok(Token::Int(2014)));
    assert_eq!(scanner.next_token(), Ok(Token::Dot));
    assert_eq!(scanner.next_token(), Ok(Token::Int(3)));
    assert_eq!(scanner.next_token(), Ok(Token::Dot));
    assert_eq!(scanner.next_token(), Ok(Token::Int(1)));    
    assert_eq!(scanner.next_token(), Ok(Token::TimeSep));
    assert_eq!(scanner.next_token(), Ok(Token::Int(0)));
    assert_eq!(scanner.next_token(), Ok(Token::Colon));
    assert_eq!(scanner.next_token(), Ok(Token::Int(23)));
    assert_eq!(scanner.next_token(), Ok(Token::Colon));
    assert_eq!(scanner.next_token(), Ok(Token::Int(2)));        
}

#[test]
fn parse_datetime_expr() {
    let mut parser = Parser::new("2014.03.01T00:23:02");
    let expr = parser.parse_expr().unwrap();
    let dt = Utc.ymd(2014, 3, 1).and_hms(0, 23, 02);
    assert_eq!(expr, Expr::DateTime(dt));
}

#[test]
fn lex_lessequal_filter() {
    let mut scanner = Scanner::new("c<=1");
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("c"))));
    assert_eq!(scanner.next_token(), Ok(Token::Less));
    assert_eq!(scanner.next_token(), Ok(Token::Equal));
    assert_eq!(scanner.next_token(), Ok(Token::Int(1)));        
}

#[test]
fn lex_less_filter() {
    let mut scanner = Scanner::new("c<1");
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("c"))));
    assert_eq!(scanner.next_token(), Ok(Token::Less));
    assert_eq!(scanner.next_token(), Ok(Token::Int(1)));        
}

#[test]
fn lex_ge_filter() {
    let mut scanner = Scanner::new("c>=1");
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("c"))));
    assert_eq!(scanner.next_token(), Ok(Token::Greater));
    assert_eq!(scanner.next_token(), Ok(Token::Equal));
    assert_eq!(scanner.next_token(), Ok(Token::Int(1)));        
}

#[test]
fn lex_g_filter() {
    let mut scanner = Scanner::new("c>1");
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("c"))));
    assert_eq!(scanner.next_token(), Ok(Token::Greater));
    assert_eq!(scanner.next_token(), Ok(Token::Int(1)));        
}

#[test]
fn lex_eq_filter() {
    let mut scanner = Scanner::new("c==1");
    assert_eq!(scanner.next_token(), Ok(Token::Id(String::from("c"))));
    assert_eq!(scanner.next_token(), Ok(Token::Equal));
    assert_eq!(scanner.next_token(), Ok(Token::Equal));
    assert_eq!(scanner.next_token(), Ok(Token::Int(1)));        
}

#[test]
fn parse_id_eq_int_filter() {
    let mut parser = Parser::new("c==1");
    let filter = Filter::new(Expr::Id("c".to_string()), Predicate::Equal, Expr::Int(1));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_int_eq_id_filter() {
    let mut parser = Parser::new("1==c");
    let filter = Filter::new(Expr::Int(1), Predicate::Equal, Expr::Id("c".to_string()));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_id_lesseq_int_filter() {
    let mut parser = Parser::new("c <= 1");
    let filter = Filter::new(Expr::Id("c".to_string()), Predicate::LessEqual, Expr::Int(1));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_int_lesseq_id_filter() {
    let mut parser = Parser::new("1<=c");
    let filter = Filter::new(Expr::Int(1), Predicate::LessEqual, Expr::Id("c".to_string()));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_id_less_int_filter() {
    let mut parser = Parser::new("c<1");
    let filter = Filter::new(Expr::Id("c".to_string()), Predicate::Less, Expr::Int(1));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_int_less_id_filter() {
    let mut parser = Parser::new("1<c");
    let filter = Filter::new(Expr::Int(1), Predicate::Less, Expr::Id("c".to_string()));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_id_gteq_int_filter() {
    let mut parser = Parser::new("c>=1");
    let filter = Filter::new(Expr::Id("c".to_string()), Predicate::GreaterEqual, Expr::Int(1));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_int_gteq_id_filter() {
    let mut parser = Parser::new("1>=c");
    let filter = Filter::new(Expr::Int(1), Predicate::GreaterEqual, Expr::Id("c".to_string()));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_id_gt_int_filter() {
    let mut parser = Parser::new("c>1");
    let filter = Filter::new(Expr::Id("c".to_string()), Predicate::Greater, Expr::Int(1));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_int_gt_id_filter() {
    let mut parser = Parser::new("1>c");
    let filter = Filter::new(Expr::Int(1), Predicate::Greater, Expr::Id("c".to_string()));
    assert_eq!(parser.parse_filter(), Ok(filter));
}

#[test]
fn parse_where_filters() {
    let mut parser = Parser::new("where a==1,b<=2,c>=5000000");
    let filters = parser.parse_where().unwrap().unwrap();
    assert_eq!(filters.len(), 3);
    let f = Filter::new(Expr::Id("a".to_string()), Predicate::Equal, Expr::Int(1));
    assert_eq!(filters[0], f);
    let f = Filter::new(Expr::Id("b".to_string()), Predicate::LessEqual, Expr::Int(2));
    assert_eq!(filters[1], f);    
    let f = Filter::new(Expr::Id("c".to_string()), Predicate::GreaterEqual, Expr::Int(5_000_000));    
    assert_eq!(filters[2], f);    
}

#[test]
fn parse_where_empty() {
    let mut parser = Parser::new("");
    let filters = parser.parse_where();
    assert_eq!(filters, Ok(None));
}
}