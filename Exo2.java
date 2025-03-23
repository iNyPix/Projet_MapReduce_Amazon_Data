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
import java.util.TreeMap;

public class Exo2 {
   /*
    * Mapper
    */
    public static class Exo2Mapper extends Mapper<Object, Text, Text, Text> {
            @Override
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // on saute la premiere ligne qui contient les attributs
                if (key.toString().equals("0") && value.toString().contains("marketplace")) {
                    return;
                }

                String line = value.toString();
                String[] attributes = line.split("\t");

                if (attributes.length >= 2) {
                    String productCategorie = attributes[6];
                    String productReview = attributes[13];
                    String starRating = attributes[7];

                    // Convertir en type Hadoop
                    Text mapKey = new Text(productCategorie);
                    Text valueComment = new Text("0");
                    if (productReview != null) {
                        valueComment = new Text("1");
                    }

                    Text valueRating = new Text("rating:" + starRating);

                    // Sortie des couples k,v
                    context.write(mapKey, valueComment);
                    context.write(mapKey, valueRating);
                }
            }
        }
    /*
     * Reducer
     */
    public static class Exo2Reducer extends Reducer<Text, Text, Text, Text> {
        // Utilisation d'une TreeMap pour trier les commentaires par ordre decroissant
        TreeMap<Integer, String> commentMap = new TreeMap<>(Comparator.reverseOrder());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double avgRating = 0;
            int nbrComment = 0;
            int numValues = 0;

            // Ajouter tous les etoiles et les commentaires
            for (Text value : values) {
                if (value.toString().contains("rating:")) {
                    avgRating += Double.parseDouble(value.toString().substring(7));
                    numValues++;
                } else {
                    nbrComment++;
                }
            }

            if (numValues > 0) // Verification de la division par zero
                avgRating /= numValues;

            // Le nombre de commentaire devient une clef et le resultat une valeur
            String result = "Number of comments: " + nbrComment + "\tAverage rating: " + avgRating;
            commentMap.put(nbrComment, key.toString() + "\t" + result);
        }

        // Methode Hadoop appelee a la fin de la fonction reduce
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String str : commentMap.values()) {
                context.write(null, new Text(str)); // Ecriture du context comme exercice1
            }
        }
    }
    /*
     * Main
     */
    public static void main(String[] args) throws Exception {
            // on verifie que les chemins des donnees et resultats sont fournis
            if (args.length != 2) {
                System.err.println("Syntaxe : Exo2 <input path> <output path>");
                System.exit(-1);
            }

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "sortedComment");

            job.setJarByClass(Exo2.class);

            // on connecte les entree et sorties sur les repertoires passes en parametre
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Exo2Mapper.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // definir la classe qui realise le map
            job.setMapperClass(Exo2Mapper.class);
            // definir la classe qui realise le reduce
            job.setReducerClass(Exo2Reducer.class);

            // definir le format de la sortie
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // lancer l'exec
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}