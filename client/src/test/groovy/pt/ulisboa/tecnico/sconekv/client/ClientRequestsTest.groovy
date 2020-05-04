package pt.ulisboa.tecnico.sconekv.client

import org.zeromq.ZContext
import pt.ulisboa.tecnico.sconekv.client.db.SconeClient
import pt.ulisboa.tecnico.sconekv.client.db.Transaction
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException
import spock.lang.Specification

class ClientRequestsTest extends Specification {

    ZContext context
    SconeClient client

    def setup() {
        context = new ZContext()
        client = new SconeClient(context, "localhost")
    }

    def "client commits transaction"() {
        given:
        def key = "client-commits-tx"
        def value = "client-commits-tx-value".getBytes()
        when:
        Transaction tx = client.newTransaction()
        tx.write(key, value)
        tx.commit()
        then: 'the value is updated'
        Transaction check = client.newTransaction()
        check.read(key) == value
    }

    def "client tries to abort a transaction after committing"() {
        given:
        def key = "client-aborts-after-commit-tx"
        def value = "client-aborts-after-commit-tx-value".getBytes()
        Transaction tx = client.newTransaction()
        tx.write(key, value)
        tx.commit()
        when:
        tx.abort()
        then: 'an exception is thrown'
        thrown(InvalidTransactionStateChangeException)
    }

    def "client doesn't commit transaction"() {
        given:
        def key = "client-does-not-commit-tx"
        def value = "client-does-not-commit-tx-value".getBytes()
        when:
        Transaction tx = client.newTransaction()
        tx.write(key, value)
        then: 'the value is not updated'
        Transaction check = client.newTransaction()
        check.read("bar") != value
    }

    def "client aborts transaction"() {
        given:
        def key = "client-aborts-tx"
        def value = "client-aborts-tx-value".getBytes()
        when:
        Transaction tx = client.newTransaction()
        tx.write(key, value)
        tx.abort()
        then: 'the value is not updated'
        Transaction check = client.newTransaction()
        check.read(key) != value
    }

    def "client aborts transaction and tries to perform additional operations"() {
        given:
        def key = "client-aborts-tx"
        def value = "client-aborts-tx-value".getBytes()
        Transaction tx = client.newTransaction()
        tx.write(key, value)
        tx.abort()
        when:
        tx.read(key)
        then: 'an exception is thrown'
        thrown(InvalidTransactionStateChangeException)
    }

    def "client read-my-write"() {
        given:
        def key = "client-read-my-write"
        def value = "client-read-my-write-value".getBytes()
        Transaction tx = client.newTransaction()
        when:
        tx.write(key, value)
        then: 'client reads the write'
        tx.read(key) == value
    }

    def "repeatable read"() {
        given:
        def key = "client-repeatable-read"
        def value = "client-repeatable-read-value".getBytes()
        Transaction tx1 = client.newTransaction()
        Transaction tx2 = client.newTransaction()
        when:
        tx2.read(key)
        tx1.write(key, value)
        tx1.commit()
        then: 'client reads the previous value'
        tx2.read(key) != value
    }

    def "forward freshness"() {
        given:
        def key1 = "client-forward-freshness-1"
        def key2 = "client-forward-freshness-2"
        def value1 = "client-forward-freshness-value-1".getBytes()
        def value2 = "client-forward-freshness-value-2".getBytes()
        Transaction tx1 = client.newTransaction()
        Transaction tx2 = client.newTransaction()
        when:
        tx1.write(key1, value1)
        tx2.write(key2,value2)
        tx2.commit()
        then: 'client reads the fresh write'
        tx1.read(key2) == value2
    }
}
