#!/usr/bin/python

__author__ = 'tiv'

import ldap
import MySQLdb
from ConfigParser import ConfigParser
from subprocess import call

mysql = ['localhost',  # host
         'postfix',  # user
         'postfix',  # password
         'postfix']  # schema


def main():
    try:
        emails = []
        domains = []
        connection = []
        cp = ConfigParser()
        cp.read('postfix-mysql.conf')
        for i in cp._sections:
            connection = [cp.get(i, 'dc'), cp.get(i, 'user'), cp.get(i, 'pass'), cp.get(i, 'dn'), cp.get(i, 'host')]
            print('Processing LDAP server ' + connection[0] + ':')
            basedn = connection[3]
            nexthop = connection[4]
            lc = ldapconnection(connection)
            ls = ldapsearch(lc, basedn)
            rl = resultlist(ls)
            emails.extend(rl[0])
            for domain in rl[1]:
                domains.append([domain, nexthop])
            print('Processing of LDAP server ' + connection[0] + ' completed.')
        createdb(emails, domains, mysql)
        print('Running postmap...')
        call(['postmap', 'hash:/etc/postfix/sender_access'])
        call(['postmap', 'hash:/etc/postfix/recipient_access'])
        call(['postmap', 'hash:/etc/postfix/virtual'])
        print('Running postmap successfully!')
        print('Operation completed successfully!')
    except:
        print('Error processing of LDAP server ' + connection[0] + '!')
        pass


def ldapconnection(ldapserver):
    try:
        print(' Trying to connect to LDAP server ' + ldapserver[0] + '...')
        ldapconnection = ldap.initialize('ldap://' + ldapserver[0])
        ldapconnection.simple_bind_s(ldapserver[1], ldapserver[2])
        ldapconnection.protocol_version = ldap.VERSION3
        ldapconnection.set_option(ldap.OPT_REFERRALS, 0)
        print(' Connection to LDAP server ' + ldapserver[0] + ' succesfull.')
    except:
        print('Error connecting to LDAP server ' + ldapserver[0] + '!')
        pass
    return ldapconnection


def ldapsearch(ldapconnection, basedn):
    try:
        print(' Sending LDAP query request...')
        scope = ldap.SCOPE_SUBTREE
        filter = '(&(proxyAddresses=smtp:*)(!(objectClass=contact)))'
        attributes = ['proxyAddresses']
        searchresults = ldapconnection.search_s(basedn, scope, filter, attributes)
        print(' LDAP query request results received.')
    except:
        print('Error sending LDAP query request!')
        pass
    return searchresults


def resultlist(searchresults):
    try:
        print(' Processing LDAP query results...')
        emails = []
        domains = []
        for i in range(len(searchresults)):
            try:
                for j in range(len(searchresults[i][1]['proxyAddresses'])):
                    r = searchresults[i][1]['proxyAddresses'][j].lower()
                    if 'smtp:' in r:
                        email = r[5:]
                        emails.append(email)
                        domain = email.split("@")[1]
                        domains.append(domain)
            except:
                pass
        print(' LDAP query results processed.')
    except:
        print('Error processing LDAP query results!')
        pass
    return removedublicates(emails), removedublicates(domains)


def createdb(emails, domains, mysql):
    try:
        print('Connecting to DB ' + mysql[3] + '...')
        try:
            db = MySQLdb.connect(host=mysql[0], user=mysql[1], passwd=mysql[2])
            cursor = db.cursor()
            sql = 'CREATE SCHEMA IF NOT EXISTS ' + mysql[3]
            cursor.execute(sql)
            db.commit()
        except:
            pass
        try:
            db = MySQLdb.connect(host=mysql[0], user=mysql[1], passwd=mysql[2], db=mysql[3])
            cursor = db.cursor()
        except:
            print('Error connecting to DB ' + mysql[3] + '!')
        print(' Check schemas and tables...')
        sql = ['CREATE TABLE IF NOT EXISTS ' + mysql[3] + '.relay_users (id INT NOT NULL, email LONGTEXT NULL, PRIMARY KEY (id))',
               'CREATE TABLE IF NOT EXISTS ' + mysql[3] + '.relay_domains (id INT NOT NULL, name LONGTEXT NULL, nexthop LONGTEXT NULL, PRIMARY KEY (id))',
               'TRUNCATE ' + mysql[3] + '.relay_users',
               'TRUNCATE ' + mysql[3] + '.relay_domains']
        for i in range(len(sql)):
            cursor.execute(sql[i])
            db.commit()
        print(' Inserting domains...')
        for i in range(len(domains)):
            sql = 'INSERT INTO postfix.relay_domains (id, name, nexthop)' \
                  'VALUES ("' + str(i) + '", "' + domains[i][0] + '", "smtp:[' + domains[i][1] + ']")'
            cursor.execute(sql)
            db.commit()
        print(' Inserting emails...')
        for i in range(len(emails)):
            sql = 'INSERT INTO postfix.relay_users (id, email)' \
                  'VALUES ("' + str(i) + '", "' + emails[i] + '")'
            cursor.execute(sql)
            db.commit()
        db.close()
        print('Connection to DB ' + mysql[3] + ' closed.')
    except:
        print('Error while operating with DB ' + mysql[3] + '!')
        pass


def removedublicates(input):
    seen = set()
    seen_add = seen.add
    return [x for x in input if not (x in seen or seen_add(x))]


if __name__ == '__main__':
    main()