// BELASSEL Meryem
// NICOLLE Thomas

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;


public class Exo3 {

   /*
    * Mapper
    */
    public static class Exo3Mapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // on saute la premiere ligne qui contient les attributs
            if (key.toString().equals("0") && value.toString().contains("marketplace")) {
                return;
            }

            String line = value.toString();
            String[] attributes = line.split("\t");

            if (attributes.length >= 2) {
                String customerId = attributes[1];
                String productCategory = attributes[6];

                // Convertir en type Hadoop
                Text mapKey = new Text(customerId);
                Text customerProducts= new Text("product:" + productCategory);

                // Sortie des couples k,v
                context.write(mapKey, customerProducts);
            }
        }
    }

    /*
     * Reducer
     */
    public static class Exo3Reducer extends Reducer<Text, Text, Text, Text> {
        // Utilisation d'une TreeMap pour trier les categorie les plus achetees
        TreeMap<String, Integer> productMap = new TreeMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> nbrProductPerCategoryMap = new HashMap<>(); // Nombre total de produits par categorie de produit
            int nbrProducts = 0;

            // Recuperer le nombre de produit du client
            for (Text value : values) {
                if (value.toString().contains("product:")) {
                    String productCategory = value.toString().substring(8);
                    nbrProductPerCategoryMap.put(
                        productCategory, 
                        nbrProductPerCategoryMap.getOrDefault(productCategory, 0) + 1 // Incrementer le nombre de produits achetes par categorie
                    );
                    nbrProducts++; // Incrementer le nombre total de produits achetes par le client
                }
            }

            // Si le client a achete au moins 10 produits
            if (nbrProducts >= 10) { 
                for (Map.Entry<String, Integer> set : nbrProductPerCategoryMap.entrySet()) {
                    String category = set.getKey();
                    int nbrProduct = set.getValue();


                    productMap.put(
                        category, 
                        productMap.getOrDefault(category, 0) + nbrProduct
                    );
                }
            }
        }

        // Methode Hadoop appelee a la fin de la fonction reduce
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Tri des categories globales par valeur (nombre de produits)
            productMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) // Trier par nombre decroissant
                .limit(50) // Limiter aux 50 premiers
                .forEach(entry -> { // Pour chaque categorie
                    try {
                        context.write(new Text("Category: " + entry.getKey()), new Text("Quantity: " + entry.getValue().toString()));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        }
    }  

    /*
     * Main
     */
    public static void main(String[] args) throws Exception {
        // on verifie que les chemins des donnees et resultats sont fournis
        if (args.length != 2) {
            System.err.println("Syntaxe : Exo3 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top50MostPurchasedProductType");

        job.setJarByClass(Exo3.class);

        // on connecte les entree et sorties sur les repertoires passes en parametre
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Exo3Mapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // definir la classe qui realise le map
        job.setMapperClass(Exo3Mapper.class);
        // definir la classe qui realise le reduce
        job.setReducerClass(Exo3Reducer.class);

        // definir le format de la sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // lancer l'exec
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}