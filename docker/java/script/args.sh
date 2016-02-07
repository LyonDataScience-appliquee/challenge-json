# file args.sh
#!/bin/sh

if [ ! -z "$POSTGRESQL_ADDRESS" ]
then
  if [ ! -z "$POSTGRESQL_USER" ]
  then
	  if [ ! -z "$POSTGRESQL_DBPASSWORD" ]
	  then
	    javac JSONParser.java
	    java JSONParser $POSTGRESQL_DBADDRESS $POSTGRESQL_DBUSER $POSTGRESQL_DBPASSWORD
    else
      echo "Mot de passe manquant"
    fi
  else
    echo "Nom d'utilisateur manquant"
  fi
else
  echo "Adresse de la base de données manquante"
fi
