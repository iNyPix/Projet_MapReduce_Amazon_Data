// BELASSEL Meryem
// NICOLLE Thomas

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Exo1 {
    /*
     * Mapper
     */
    public static class Exo1Mapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // on saute la premiere ligne qui contient les attributs
            if (key.toString().equals("0") && value.toString().contains("marketplace")) {
                return;
            }

            String line = value.toString();
            String[] attributes = line.split("\t");

            if (attributes.length >= 2) {
                // ligne des fichiers AmazonReview : 
                // [marketplace, customer_id, review_id, product_id,
                // product_parent, product_title, product_category, star_rating, helpful_votes,
                // total_votes, vine, verified_purchase, review_headline, review_body,
                // review_date]
                String productCategorie = attributes[6];
                String productReview = attributes[13];
                String starRating = attributes[7];

                // Convert to Hadoop types
                Text mapKey = new Text(productCategorie);
                Text valueComment = new Text("0");
                if (productReview != null) {
                    valueComment = new Text("1");
                }

                Text valueRating = new Text("rating:" + starRating);

                // sortie des couples k,v
                context.write(mapKey, valueComment);
                context.write(mapKey, valueRating);
            }
        }
    }
    /*
    * Reducer
    */

    public static class Exo1Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double avgRating = 0;
            int nbrComment = 0;
            int numValues = 0; // compteur

            // Add up all the ratings and comment
            for (Text value : values) {
                if (value.toString().contains("rating:")) {
                    avgRating += Double.parseDouble(value.toString().substring(7));
                    numValues++;
                } else {
                    nbrComment++;
                }
            }

            avgRating /= numValues;
            DoubleWritable average = new DoubleWritable(avgRating);

            context.write(key, new Text("average rating: " + average + "\t" + "number of comments: " + nbrComment));
        }
    }
    /*
     * Main
    */
    public static void main(String[] args) throws Exception {
        // on verifie que les chemins des donnees et resultats sont fournis
        if (args.length != 2) {
            System.err.println("Syntaxe : Exo1 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "productCommentRating");

        job.setJarByClass(Exo1.class);

        // on connecte les entree et sorties sur les repertoires passes en parametre
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Exo1Mapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // definir la classe qui realise le map
        job.setMapperClass(Exo1Mapper.class);
        // definir la classe qui realise le reduce
        job.setReducerClass(Exo1Reducer.class);

        // definir le format de la sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // lancer l'exec
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
