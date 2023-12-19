
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;

import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;


import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;


import javax.swing.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.util.Arrays.asList;


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

            hyperGraph.addHyper(atom.getRelationName() , hyperedge);
        }

        return hyperGraph;
    }
}
class HyperEdge {
    private Set<String> nodes;
    private String name;

    HyperEdge(String name , Set<String> nodes){
        this.name = name ;
        this.nodes = nodes;
    }
    public String getName(){
        return this.name;
    }
    public Set<String> getNodes(){
        return this.nodes;
    }

}
class HyperGraph {

    private JoinTree joinTree;

    private List<HyperEdge> hyperEdges;

    public HyperGraph() {
        this.hyperEdges = new ArrayList<>();
    }



    public void addHyper(String name , Set<String> nodes){
        this.hyperEdges.add(new HyperEdge(name , nodes));
    }

    public void printHyperGraph() {
        System.out.println("Hyperedges:");
        for (HyperEdge hyperedge : hyperEdges) {
            System.out.println(hyperedge.getName());
            System.out.println(hyperedge.getNodes());
            System.out.println("-------------------------");
        }


    }

    public JoinTree getJoinTree(){
        return joinTree;
    }



    // Check if a set is an ear in the hypergraph
    private boolean isEar(Set<String> possibleEar,Set<String> possibleWitness, Set<HyperEdge> hypergraph) {

        if (isExclusive(possibleEar, hypergraph)) {
            return true;
        }
        if (possibleEar.equals(possibleWitness)) return false;
        for (String vertice : possibleEar){
            if (!possibleWitness.contains(vertice)) {
                for (HyperEdge hyperedge : hypergraph) {
                    if (hyperedge.getNodes() != possibleEar && hyperedge.getNodes() != possibleWitness && hyperedge.getNodes().contains(vertice)) return false;
                }
            }
        }
        return true;
    }

    // Check if an edge's nodes are exclusive to that node
    private boolean isExclusive(Set<String> hyperedge, Set<HyperEdge> hypergraph){
        for (String node : hyperedge){
            for (HyperEdge otherEdge : hypergraph){
                if (!otherEdge.getNodes().equals(hyperedge) && otherEdge.getNodes().contains(node)) return false;
            }
        }
        return true ;
    }

    public boolean isAcyclic() {
        Set<HyperEdge> gyoReduction = new HashSet<>(this.hyperEdges);
        boolean done = false;
        JoinTree tree = new JoinTree();
        while (!done){
            Iterator<HyperEdge> iterator = gyoReduction.iterator();
            int initialSize = gyoReduction.size();
            while (iterator.hasNext()) {
                HyperEdge possibleEar = iterator.next();
                System.out.println(possibleEar.getName());

                boolean removable = false;
                for (HyperEdge possibleWitness : gyoReduction){
                    if (isEar(possibleEar.getNodes() ,possibleWitness.getNodes() , gyoReduction)){
                        removable = true;

                        if(tree.getRoot() == null) {
                            JoinTreeNode newlyAdded = new JoinTreeNode(possibleWitness.getNodes(), null,possibleWitness.getName());
                            tree.setRoot(newlyAdded);
                            tree.addChild(possibleWitness.getNodes(), possibleEar.getNodes() , possibleWitness.getName());

                        }
                        else {
                            JoinTreeNode parentNode = tree.findNodeByAtom(possibleEar.getNodes(), tree.getRoot());
                            if (parentNode != null){
                                tree.addChild(possibleEar.getNodes(), possibleWitness.getNodes() , possibleEar.getName());
                            }
                            else tree.addChild(possibleWitness.getNodes(), possibleEar.getNodes() , possibleWitness.getName());

                            System.out.println(possibleWitness.getName());
                        }
                        tree.printJoinTree();
                        break;

                    }
                }
                if (removable) {
                    iterator.remove();
                    gyoReduction.remove(possibleEar);
                    if (gyoReduction.size() == 1 ) {this.joinTree = tree; return true;}
                    break;  // break the iterated loop
                }

            }
            if (gyoReduction.size() == initialSize) done = true ; // no ear was removed, we stop here
        }

        return false;

    }


}



class JoinTreeNode {
    Set<String> atom;
    String name;
    Set<JoinTreeNode> children = new HashSet<>();
    JoinTreeNode parent;

    JoinTreeNode(Set<String> atom , JoinTreeNode parent , String name) {
        this.atom = atom;
        this.parent = parent;
        this.name = name ;
    }

    public void setName(String name){
        this.name = name;
    }
}

class JoinTree {
    JoinTreeNode root;
    int size = 0 ;

    JoinTree(Set<String> rootAtom , String name) {
        this.root = new JoinTreeNode(rootAtom , null , name);
    }

    JoinTree() {
        this.root = null ;
    }

    public JoinTreeNode getRoot() {
        return root;
    }
    public int getSize() { return this.size; }

    public void setRoot(JoinTreeNode root){
        this.root = root; this.size = 1 ;
    }
    // Traverse the tree to find the parent and add the child
    void addChild(Set<String> parentAtom, Set<String> childAtom , String parentName) {
        JoinTreeNode parentNode = findNodeByAtom(parentAtom, root);
        JoinTreeNode childNode = new JoinTreeNode(childAtom , parentNode , parentName);
        parentNode.children.add(childNode);
        this.size++ ;
    }

    // Helper method to find a node by atom content in the tree
    public JoinTreeNode findNodeByAtom(Set<String> targetAtom, JoinTreeNode currentNode) {
        if (currentNode == null) {
            return null;  // Node not found
        }

        if (currentNode.atom.equals(targetAtom)) {
            return currentNode;  // Node found
        }

        // Recursively search in children
        for (JoinTreeNode child : currentNode.children) {
            JoinTreeNode result = findNodeByAtom(targetAtom, child);
            if (result != null) {
                return result;  // Node found in the subtree
            }
        }

        return null;  // Node not found
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
    private static List<VectorSchemaRoot> loadCsvFile(String fileName) {
        String uri = "file:/home/zak/IdeaProjects/Apache_Arrow_proj/src/main/resources/data/" + fileName;

        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(),
                        FileFormat.CSV, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
        ) {
            List<VectorSchemaRoot> combinedRoot = new ArrayList<>();

            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    combinedRoot.add(root);
                }
            }
            return combinedRoot;
            // return the single VectorSchemaRoot combining the whole thing
        } catch (Exception e) {
            e.printStackTrace();
        }


        return null; // Return null in case of an error or if no data is loaded
    }


    public static void fullReducer(JoinTree tree) {
        List<PairChildParent> semiJoinsPostOrder = postOrderTraversal(tree.getRoot());  // post order semi joins first
        //List<PairChildParent> semiJoinsPreOrder = preOrderTraversal(tree.getRoot());         // pre order semi joins

        for (PairChildParent pair : semiJoinsPostOrder){
            System.out.println("ICII");
            // create empty csv file : name = old_name + '+'
            String path = "src/main/resources/data/" + pair.parent.name.toLowerCase() + "+.csv";
            String uri_left = "file:/home/zak/IdeaProjects/Apache_Arrow_proj/src/main/resources/data/" + pair.parent.name.toLowerCase() + ".csv";
            String uri_right = "file:/home/zak/IdeaProjects/Apache_Arrow_proj/src/main/resources/data/" + pair.child.name.toLowerCase() +".csv";
            try {
                // Create an empty file or overwrite if it already exists
                Files.write(Path.of(path.toLowerCase()), "".getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                System.out.println("Empty CSV file created successfully.");
            } catch (IOException e) {
                e.printStackTrace();
            }
            try(
                BufferAllocator allocator_left = new RootAllocator();
                DatasetFactory datasetFactory_left = new FileSystemDatasetFactory(allocator_left, NativeMemoryPool.getDefault(), FileFormat.CSV,uri_left);
                Dataset dataset_left = datasetFactory_left.finish();
                Scanner scanner_left = dataset_left.newScan(new ScanOptions(32768));
                ArrowReader reader_left = scanner_left.scanBatches()
            ){
                while (reader_left.loadNextBatch()) {
                    try (VectorSchemaRoot root_left = reader_left.getVectorSchemaRoot()) {
                        // First batch on first file is loaded
                        try(
                                BufferAllocator allocator_right = new RootAllocator();
                                DatasetFactory datasetFactory_right = new FileSystemDatasetFactory(allocator_right, NativeMemoryPool.getDefault(), FileFormat.CSV, uri_right);
                                Dataset dataset_right = datasetFactory_right.finish();
                                Scanner scanner_right = dataset_right.newScan(new ScanOptions(32768));
                                ArrowReader reader_right = scanner_right.scanBatches()
                        ){
                            while (reader_right.loadNextBatch()) {
                                try (VectorSchemaRoot root_right = reader_right.getVectorSchemaRoot()) {
                                    // First batch on second file is loaded
                                    List<String> common_cols = findCommonColumns(root_left , root_right);
                                    try(VectorSchemaRoot result = semiJoin(root_left , root_right, common_cols.get(0))){            // SemiJoined columns is only the first common one
                                        writeVSR_to_CSV(path.toLowerCase() , result);
                                    }

                                }
                            }
                        }catch (Exception e ){
                            e.printStackTrace();
                        }

                    }
                }
                // update the pairs here
                for (PairChildParent update_pair : semiJoinsPostOrder){
                    if (update_pair.parent.name.equals(pair.parent.name)) {
                        pair.parent.setName(pair.parent.name + "+");
                    }
                    else if(update_pair.child.name.equals(pair.parent.name)){
                        pair.child.setName(pair.parent.name + "+");
                    }
                }
            }catch (Exception e ){
                e.printStackTrace();
            }
        }

        /*
        // POST-ORDER TRAVERSAL SEMI JOINS HERE
        for (PairChildParent pair: semiJoinsPostOrder){
            for (VectorSchemaRoot leftSchema: database){
                if (leftSchema.getSchema().toString().equals(pair.parent.name)){
                    for (VectorSchemaRoot rightSchema : database){
                        leftSchema = semiJoin(leftSchema , rightSchema , pair.commonColumn);
                    }
                }
            }
        }
        */
    }
    public static void writeVSR_to_CSV(String path_csv , VectorSchemaRoot vsr){
        try (FileWriter csvWriter = new FileWriter(path_csv)) {
            // Write header row
            for (FieldVector fieldVector : vsr.getFieldVectors()) {
                csvWriter.append(fieldVector.getField().getName());
                csvWriter.append(",");
            }
            csvWriter.append("\n");

            // Write data rows
            int numRows = vsr.getRowCount();
            System.out.println(numRows);
            for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                for (FieldVector fieldVector : vsr.getFieldVectors()) {
                    // Extract data from the vector
                    String cellValue = extractCellValue(fieldVector, rowIndex);

                    // Write the cell value to the CSV file
                    csvWriter.append(cellValue);
                    csvWriter.append(",");
                }
                csvWriter.append("\n");
            }

            System.out.println("Data written to the CSV file successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static String extractCellValue(FieldVector fieldVector, int rowIndex) {
        // Replace this with the appropriate logic to extract the cell value based on the vector type
        if (fieldVector instanceof VarCharVector) {
            VarCharVector varCharVector = (VarCharVector) fieldVector;
            return varCharVector.getObject(rowIndex).toString();
        } else {
            // Handle other vector types accordingly
            return "N/A";
        }
    }


    private static List<String> findCommonColumns(VectorSchemaRoot leftTable, VectorSchemaRoot rightTable) {
        // Find common columns between the two tables based on their schemas
        List<Field> leftFields = leftTable.getSchema().getFields();
        List<Field> rightFields = rightTable.getSchema().getFields();

        List<String> commonColumns = new ArrayList<>();
        for (Field leftField : leftFields) {
            for (Field rightField : rightFields) {
                if (leftField.getName().equals(rightField.getName())) {
                    commonColumns.add(leftField.getName());
                    break;  // Break out of inner loop when a common column is found
                }
            }
        }

        return commonColumns;
    }

    public static VectorSchemaRoot semiJoin(VectorSchemaRoot leftTable, VectorSchemaRoot rightTable, String joinColumnName) {
        // Get the join columns from both tables
        ValueVector joinColumnLeft = leftTable.getVector(joinColumnName);
        ValueVector joinColumnRight = rightTable.getVector(joinColumnName);

        // Perform the semi-join and get the matching indices
        List<Integer> matchingIndices = performSemiJoin(joinColumnLeft, joinColumnRight);

        // Create a new schema and vectors for the result
        Schema resultSchema = new Schema(leftTable.getSchema().getFields());
        List<FieldVector> resultVectors = new ArrayList<>();

        // Populate the result vectors using the matching indices
        for (Field field : leftTable.getSchema().getFields()) {
            ValueVector originalVector = leftTable.getVector(field.getName());
            FieldVector resultVector = originalVector.getField().createVector(originalVector.getAllocator());
            resultVector.allocateNewSafe();
            for (int index : matchingIndices) {
                resultVector.copyFrom(index, 0, originalVector);
            }
            resultVectors.add(resultVector);
        }

        // Create the result VectorSchemaRoot
        return new VectorSchemaRoot(resultSchema.getFields(), resultVectors);
    }

    private static List<Integer> performSemiJoin(ValueVector leftColumn, ValueVector rightColumn) {
        // Implement your semi-join logic here
        // Compare the values in leftColumn and rightColumn, and return the matching indices
        // This is a simplistic example, and you may need to handle different data types and conditions
        List<Integer> matchingIndices = new ArrayList<>();
        for (int i = 0; i < leftColumn.getValueCount(); i++) {
            for (int j = 0; j < rightColumn.getValueCount(); j++) {
                if (leftColumn.getObject(i).equals(rightColumn.getObject(j))) {
                    matchingIndices.add(i);
                    break;  // Break out of inner loop when a match is found
                }
            }
        }
        return matchingIndices;
    }

    static List<PairChildParent> postOrderTraversal(JoinTreeNode root) {
        List<PairChildParent> result = new ArrayList<>();
        postOrderTraversalHelper(root, result);
        return result;
    }

    static void postOrderTraversalHelper(JoinTreeNode node, List<PairChildParent> result) {
        if (node == null) {
            return;
        }

        // Traverse children first
        for (JoinTreeNode child : node.children) {
            postOrderTraversalHelper(child, result);
        }

        // Visit the current node after all children
        if (node.parent != null) {
            result.add(new PairChildParent(node , node.parent));
        }
    }


    static List<PairChildParent> preOrderTraversal(JoinTreeNode root) {
        List<PairChildParent> result = new ArrayList<>();
        preOrderTraversalHelper(root, result);
        return result;
    }

    static void preOrderTraversalHelper(JoinTreeNode node, List<PairChildParent> result) {
        if (node == null) {
            return;
        }

        // Visit the current node before its children
        if (node.parent != null) {
            // Perform operations between each parent and its child
            PairChildParent pair = new PairChildParent(node , node.parent);
            result.add(pair);
        }

        // Traverse children
        for (JoinTreeNode child : node.children) {
            preOrderTraversalHelper(child, result);
        }
    }

    static class PairChildParent {
        JoinTreeNode child;
        JoinTreeNode parent;

        String commonColumn ;

        PairChildParent(JoinTreeNode child, JoinTreeNode parent) {
            this.child = child;
            this.parent = parent;
            for (String field : child.atom){
                if (parent.atom.contains(field)) {
                    // First common column is taken, others ignored
                    commonColumn = field;
                    break;
                }
            }
        }

        @Override
        public String toString() {
            return "(" + child.atom + ", " + parent.atom + ")";
        }
    }

    public static void combine(VectorSchemaRoot targetRoot, List<VectorSchemaRoot> sourceRoots) throws IOException {
        try(BufferAllocator allocator = new RootAllocator()){
            if (targetRoot == null){
                targetRoot = VectorSchemaRoot.create(sourceRoots.get(0).getSchema(), allocator);
            }
            for (VectorSchemaRoot sourceRoot : sourceRoots) {
                combineRoots(targetRoot, sourceRoot);
            }
        }

    }

    private static void combineRoots(VectorSchemaRoot targetRoot, VectorSchemaRoot sourceRoot) throws IOException {
        Schema schema = targetRoot.getSchema();
        for (FieldVector targetVector : targetRoot.getFieldVectors()) {
            FieldVector sourceVector = sourceRoot.getVector(targetVector.getName());
            if (sourceVector != null) {
                TransferPair transferPair = targetVector.makeTransferPair(sourceVector);
                transferPair.transfer();
            }
        }
        targetRoot.setRowCount(targetRoot.getRowCount() + sourceRoot.getRowCount());
    }

    public static void main(String[] args) throws IOException {

        // Example query: Answer() :-
        //                  Beers(beer_id, brew_id, beer, abv, ibu, ounces, style, style2),
        //                  Styles(style_id, cat_id, style),
        //                  Categories(cat_id, 'Belgian and French Ale').

        // Beers
        Term beerId = new Term("beer_id", true);
        Term brewId = new Term("brew_id", true);
        Term beer = new Term("beer", true);
        Term abv = new Term("abv", true);
        Term ibu = new Term("ibu", true);
        Term ounces = new Term("ounces", true);
        Term style = new Term("style", true);
        Term style2 = new Term("style2", true);

        List<Term> beersTerms = List.of(beerId, brewId, beer, abv, ibu, ounces, style, style2);
        Atom beersAtom = new Atom("Beers", beersTerms);

        // Styles
        Term styleId = new Term("style_id", true);
        Term catIdStyle = new Term("cat_id", true);
        Term styleTerm = new Term("style", true);

        List<Term> stylesTerms = List.of(styleId, catIdStyle, styleTerm);
        Atom stylesAtom = new Atom("Styles", stylesTerms);

        // Categories
        Term catIdCategories = new Term("cat_id", true);
        Term belgianFrenchAle = new Term("Belgian and French Ale", false);

        List<Term> categoriesTerms = List.of(catIdCategories, belgianFrenchAle);
        Atom categoriesAtom = new Atom("Categories", categoriesTerms);

        // Full QUERY
        ConjunctiveQuery query = new ConjunctiveQuery(new Atom("Answer", List.of()), List.of(beersAtom, stylesAtom, categoriesAtom));


        HyperGraph hyperGraph = query.buildHyperGraph();
        //hyperGraph.printHyperGraph();
        if (hyperGraph.isAcyclic()){
            JoinTree treeCQ = hyperGraph.getJoinTree();
            treeCQ.printJoinTree();
            fullReducer(treeCQ);


        }



        // Load the database using Apache Arrow here



        String uriBeers = "beers.csv";
        String uriLocations = "locations.csv";
        String uriBreweries = "breweries.csv";
        String uriCategories = "categories.csv";
        String uriStyles = "styles.csv";





        // HERE CALL THE YANNAKAKIS :
        // IntialQuery --> Hypergraph --> Acyclicity ? --> JoinTree --> FullReducer --> Query the new DB
    }



}