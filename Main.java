import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;


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
class HyperGraph {
    private Set<Set<String>> hyperedges;
    private Set<String> nodes;
    private JoinTree joinTree;

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

    public JoinTree getJoinTree(){
        return joinTree;
    }



    // Check if a set is an ear in the hypergraph
    private boolean isEar(Set<String> possibleEar,Set<String> possibleWitness, Set<Set<String>> hypergraph) {

        if (isExclusive(possibleEar, hypergraph)) {
            return true;
        }
        if (possibleEar.equals(possibleWitness)) return false;
        for (String vertice : possibleEar){
            if (!possibleWitness.contains(vertice)) {
                for (Set<String> hyperedge : hypergraph) {
                    if (hyperedge != possibleEar && hyperedge != possibleWitness && hyperedge.contains(vertice)) return false;
                }
            }
        }
        return true;
    }

    // Check if an edge's nodes are exclusive to that node
    private boolean isExclusive(Set<String> hyperedge, Set<Set<String>> hypergraph){
        for (String node : hyperedge){
            for (Set<String> otherEdge : hypergraph){
                if (!otherEdge.equals(hyperedge) && otherEdge.contains(node)) return false;
            }
        }
        return true ;
    }

    public boolean isAcyclic() {
        Set<Set<String>> gyoReduction = new HashSet<>(hyperedges);
        boolean done = false;
        JoinTree tree = new JoinTree();
        while (!done){
            Iterator<Set<String>> iterator = gyoReduction.iterator();
            int initialSize = gyoReduction.size();
            System.out.println(initialSize);
            while (iterator.hasNext()) {
                Set<String> possibleEar = iterator.next();
                System.out.println(possibleEar);
                System.out.println((" \n ------------ \n "));
                boolean removable = false;
                for (Set<String> possibleWitness : gyoReduction){
                    if (isEar(possibleEar ,possibleWitness , gyoReduction)){
                        removable = true;
                        System.out.println("\n found a witness !  : ");
                        System.out.println(possibleWitness);

                        if(tree.getRoot() == null) tree.setRoot(new JoinTreeNode(possibleWitness));
                        tree.addChild(tree.getRoot() , possibleEar);
                        break;
                    }
                }
                if (removable) {
                    iterator.remove();
                    gyoReduction.remove(possibleEar);
                    if (gyoReduction.isEmpty()) {this.joinTree = tree; return true;}
                    break;  // break the iterated loop
                }
                else System.out.println(" \n no witness \n ");
            }
            if (gyoReduction.size() == initialSize) done = true ;   // no ear was removed, we stop here
        }

        return false;

    }


}



class JoinTreeNode {
    Set<String> atom;
    Set<JoinTreeNode> children = new HashSet<>();

    JoinTreeNode(Set<String> atom) {
        this.atom = atom;
        //this.children = Set<>();
    }
}

class JoinTree {
    JoinTreeNode root;

    JoinTree(Set<String> rootAtom) {
        this.root = new JoinTreeNode(rootAtom);
    }

    JoinTree() {
        this.root = null ;
    }

    public JoinTreeNode getRoot() {
        return root;
    }

    public void setRoot(JoinTreeNode root){
        this.root = root;
    }
    void addChild(JoinTreeNode parent, Set<String> childAtom) {
        if (!parent.atom.equals(childAtom)){
            JoinTreeNode childNode = new JoinTreeNode(childAtom);
            parent.children.add(childNode);
        }

    }

    void printJoinTree() {
        if (root != null) {
            printJoinTreeNode(root, "");
        } else {
            System.out.println("JoinTree is empty.");
        }
    }

    private void printJoinTreeNode(JoinTreeNode node, String prefix) {
        System.out.println(prefix + "└─ " + node.atom);

        int childCount = node.children.size();
        int i = 1;

        for (JoinTreeNode child : node.children) {
            String childPrefix = prefix + (i == childCount ? "    " : "│   ");
            printJoinTreeNode(child, childPrefix);
            i++;
        }
    }
    
}


public class Main {
    public static void main(String[] args) {
        // Example query: Answer() :- Beers(beer_id, brew_id, beer, abv, ibu, ounces, style, style2),
        // Styles(style_id, cat_id, style), Categories(cat_id, 'Belgian and French Ale').
        
        Term beerId = new Term("beer_id", true);
        Term brewId = new Term("brew_id", true);
        Term beer = new Term("beer", true);
        Term abv = new Term("abv", true);
        Term ibu = new Term("ibu", true);
        Term ounces = new Term("ounces", true);
        Term style = new Term("style", true);
        Term style2 = new Term("style2", true);

        List<Term> beerTerms = List.of(beerId, brewId, beer, abv, ibu, ounces, style, style2);
        Atom beersAtom = new Atom("Beers", beerTerms);

        Term styleId = new Term("style_id", true);
        Term catId = new Term("cat_id", true);

        List<Term> styleTerms = List.of(styleId, catId, style);
        Atom stylesAtom = new Atom("Styles", styleTerms);

        Term catId2 = new Term("cat_id", true);
        Term category = new Term("Belgian and French Ale", false);

        List<Term> categoryTerms = List.of(catId2, category);
        Atom categoriesAtom = new Atom("Categories", categoryTerms);

        ConjunctiveQuery query = new ConjunctiveQuery(new Atom("Answer", List.of()), List.of(beersAtom, stylesAtom, categoriesAtom));

        HyperGraph hGraph = query.buildHyperGraph();
        System.out.println(hGraph.isAcyclic());
        hGraph.getJoinTree().printJoinTree();
        //query.buildHyperGraph().printHyperGraph();

    }
}