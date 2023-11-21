import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

// Represents a term that can be a variable or a constant
class Term {
    private String value;
    private boolean isVariable;

    public Term(String value, boolean isVariable) {
        this.value = value;
        this.isVariable = isVariable;
    }

    public boolean isVariable() {
        return isVariable;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return isVariable ? "?" + value : value;
    }
}

// Represents an atomic formula consisting of a relation name and a tuple of terms
class Atom {
    private String relationName;
    private List<Term> terms;

    public Atom(String relationName, List<Term> terms) {
        this.relationName = relationName;
        this.terms = terms;
    }

    public String getRelationName() {
        return relationName;
    }

    public List<Term> getTerms() {
        return terms;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(relationName).append("(");
        for (int i = 0; i < terms.size(); i++) {
            sb.append(terms.get(i));
            if (i < terms.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }
}

// Represents the entire CQ with a head atom and a set of body atoms
class ConjunctiveQuery {
    private Atom head;
    private List<Atom> body;

    public ConjunctiveQuery(Atom head, List<Atom> body) {
        this.head = head;
        this.body = body;
    }

    public Atom getHead() {
        return head;
    }

    public List<Atom> getBody() {
        return body;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(head).append(" :- ");
        for (int i = 0; i < body.size(); i++) {
            sb.append(body.get(i));
            if (i < body.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public HyperGraph buildHyperGraph() {
        HyperGraph hyperGraph = new HyperGraph();

        // Add hyperedges for body atoms
        for (Atom atom : body) {
            Set<String> hyperedge = new HashSet<>();
            for (Term term : atom.getTerms()) {
                if (term.isVariable()) {
                    hyperedge.add(term.getValue());
                }
            }
            hyperGraph.addHyperedge(hyperedge);
        }

        return hyperGraph;
    }
}
public class HyperGraph {
    private Set<Set<String>> hyperedges;
    private Set<String> nodes;

    public HyperGraph() {
        this.hyperedges = new HashSet<>();
        this.nodes = new HashSet<>();
    }

    public void addHyperedge(Set<String> hyperedge) {
        hyperedges.add(new HashSet<>(hyperedge));
        nodes.addAll(hyperedge);
    }

    public void printHyperGraph() {
        System.out.println("Hyperedges:");
        for (Set<String> hyperedge : hyperedges) {
            System.out.println(hyperedge);
        }

        System.out.println("Nodes: " + nodes);
    }

    // Check if the hypergraph is α-acyclic using the GYO algorithm
    public boolean isAlphaAcyclic() {
        Set<Set<String>> gyoReduction = new HashSet<>(hyperedges);

        boolean removed;
        do {
            removed = false;

            Iterator<Set<String>> iterator = gyoReduction.iterator();
            while (iterator.hasNext()) {
                Set<String> ear = iterator.next();

                if (isEar(ear, gyoReduction)) {
                    iterator.remove();
                    removed = true;
                }
            }
        } while (removed);

        // If the GYO-reduction is empty, the hypergraph is α-acyclic
        return gyoReduction.isEmpty();
    }

    // Check if a set is an ear in the hypergraph
    private boolean isEar(Set<String> ear, Set<Set<String>> hypergraph) {
        Set<Set<String>> subgraph = new HashSet<>(hypergraph);
        subgraph.remove(ear);

        Set<String> endpoints = new HashSet<>();
        for (Set<String> edge : subgraph) {
            endpoints.addAll(edge);
        }

        Set<String> intersection = new HashSet<>(ear);
        intersection.retainAll(endpoints);

        // An ear is a set with exactly two common endpoints with the subgraph
        return intersection.size() == 2;
    }
}

// Example usage
public class CQRepresentation {
    public static void main(String[] args) {
        // Create terms for the head atom
        List<Term> headTerms = new ArrayList<>();
        headTerms.add(new Term("y", true)); // Variable
        Atom head = new Atom("Answer", headTerms);

        // Create terms for the body atoms
        List<Term> bodyTerms1 = new ArrayList<>();
        bodyTerms1.add(new Term("u1", true)); // Variable
        bodyTerms1.add(new Term("a", false)); // Constant
        Atom bodyAtom1 = new Atom("R1", bodyTerms1);

        List<Term> bodyTerms2 = new ArrayList<>();
        bodyTerms2.add(new Term("u2", true)); // Variable
        Atom bodyAtom2 = new Atom("R2", bodyTerms2);

        // Add body atoms to a list
        List<Atom> body = new ArrayList<>();
        body.add(bodyAtom1);
        body.add(bodyAtom2);

        // Create the conjunctive query
        ConjunctiveQuery cq = new ConjunctiveQuery(head, body);

        // Print the conjunctive query
        System.out.println(cq);
    }
}