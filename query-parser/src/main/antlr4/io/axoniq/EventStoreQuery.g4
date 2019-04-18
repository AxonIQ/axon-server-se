grammar EventStoreQuery;

query : (time_constraint | expr alias_def?) ( '|' (time_constraint | expr alias_def?) )*;

time_constraint: K_LAST signed_number? time_unit;

time_unit: K_SECOND | K_MINUTE | K_HOUR | K_DAY | K_WEEK | K_MONTH | K_YEAR;


expr
 : literal_value
 | signed_number
 | identifier
 | expr_list
 | expr operator_group1 expr
 | expr operator_group2 expr
 | expr function_name expr
 | expr operator_group3 expr
 | expr operator_group4 expr
 | unary_operator expr
 | expr operator_group5 expr
 | expr operator_group6 expr
 | operator '(' ( expr alias_def? ( ',' expr alias_def? )* )? ')'
 | '(' expr ')'
 ;


expr_list: '[' expr alias_def? ( ',' expr alias_def?)* ']';

operator: function_name | operator_group1 | operator_group2 | operator_group3 | operator_group4 | operator_group5 | operator_group6;

alias_def: K_AS alias;

operator_group1:  '*' | '/' | '%' ;
operator_group2:  '+' | '-' ;
operator_group3:  '<' | '<=' | '>' | '>=' ;
operator_group4:  '=' | '!=' | '<>';
operator_group5:  K_AND;
operator_group6:  K_OR;



signed_number
 : ( '+' | '-' )? NUMERIC_LITERAL
 ;

literal_value
 : STRING_LITERAL
 ;

identifier: IDENTIFIER ('.' IDENTIFIER)*;

alias: IDENTIFIER | STRING_LITERAL;

function_name
  : IDENTIFIER | K_MINUTE | K_HOUR | K_DAY | K_WEEK | K_MONTH | K_YEAR
  ;

unary_operator
 : '-'
 | '+'
 | K_NOT
 ;

K_LAST: L A S T;

K_SECOND: S E C (O N D)? S?;
K_MINUTE: M I N (U T E)? S?;
K_HOUR: H (O U)? R S?;
K_DAY: D A Y S?;
K_WEEK: W (E E)? K S?;
K_MONTH: M O (N T H)? S?;
K_YEAR: Y (E A)? R S?;

K_AND: A N D;
K_OR: O R;

K_NULL: N U L L;
K_NOT: N O T;

K_AS: A S;

IDENTIFIER
 : [a-zA-Z_] [a-zA-Z_0-9]*
 ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

STRING_LITERAL
 : '"' ('\\' '"' | ~('\r' | '\n' | '"'))* '"'
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;



fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];