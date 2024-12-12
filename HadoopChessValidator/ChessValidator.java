package com.mapreduce.chess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class ChessValidator {

    public static class ChessMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            if (parts.length != 3) {
                context.write(new Text("Error"), new Text("Invalid input format: " + value.toString()));
                return;
            }

            String color = parts[0];
            String piece = parts[1];
            String position = parts[2];

            outputKey.set("PieceData");
            outputValue.set(color + "," + piece + "," + position);
            context.write(outputKey, outputValue);
        }
    }

    public static class ChessReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Map<String, String>> standardSetup = new HashMap<>();
        private Set<String> validPositions = new HashSet<>();
        private Map<String, String> occupiedPositions = new HashMap<>();
        private Map<String, List<String>> missingPieces = new HashMap<>();
        private Map<String, Map<String, List<String>>> validPositionsList = new HashMap<>();
        private List<String> errorsDetected = new ArrayList<>();

        @Override
        protected void setup(Context context) {
            initializeStandardSetup();
            initializeValidPositions();
            missingPieces.put("White", new ArrayList<>());
            missingPieces.put("Black", new ArrayList<>());
            validPositionsList.put("White", new LinkedHashMap<>());
            validPositionsList.put("Black", new LinkedHashMap<>());
        }

        private void initializeStandardSetup() {
            Map<String, String> whitePieces = new LinkedHashMap<>();
            Map<String, String> blackPieces = new LinkedHashMap<>();

            // White pieces
            whitePieces.put("E1", "King");
            whitePieces.put("D1", "Queen");
            whitePieces.put("A1", "Rook");
            whitePieces.put("H1", "Rook");
            whitePieces.put("C1", "Bishop");
            whitePieces.put("F1", "Bishop");
            whitePieces.put("B1", "Knight");
            whitePieces.put("G1", "Knight");
            for (char file = 'A'; file <= 'H'; file++) {
                whitePieces.put(file + "2", "Pawn");
            }

            // Black pieces
            blackPieces.put("E8", "King");
            blackPieces.put("D8", "Queen");
            blackPieces.put("A8", "Rook");
            blackPieces.put("H8", "Rook");
            blackPieces.put("C8", "Bishop");
            blackPieces.put("F8", "Bishop");
            blackPieces.put("B8", "Knight");
            blackPieces.put("G8", "Knight");
            for (char file = 'A'; file <= 'H'; file++) {
                blackPieces.put(file + "7", "Pawn");
            }

            standardSetup.put("White", whitePieces);
            standardSetup.put("Black", blackPieces);
        }

        private void initializeValidPositions() {
            for (char file = 'A'; file <= 'H'; file++) {
                for (int rank = 1; rank <= 8; rank++) {
                    validPositions.add(file + String.valueOf(rank));
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                String color = parts[0];
                String piece = parts[1];
                String position = parts[2];

                validatePiece(color, piece, position);
            }
        }

        private void validatePiece(String color, String piece, String position) {
            if (!validPositions.contains(position)) {
                errorsDetected.add("Invalid Position: \"" + color + " " + piece + " " + position + "\" - Position must be within A1 to H8.");
                return;
            }

            Map<String, String> colorSetup = standardSetup.get(color);
            String expectedPiece = colorSetup.get(position);

            if (expectedPiece != null && expectedPiece.equals(piece)) {
                // Correct piece in the correct position
                validPositionsList.get(color)
                    .computeIfAbsent(piece, k -> new ArrayList<>())
                    .add(position);
                colorSetup.remove(position);
            } else if (occupiedPositions.containsKey(position)) {
                // Position already occupied by another piece
                String existingPiece = occupiedPositions.get(position);
                errorsDetected.add("Invalid Position: \"" + color + " " + piece + " " + position + "\" - Position already occupied by \"" + existingPiece + "\".");
            } else if (expectedPiece != null) {
                // Wrong piece in a correct position (duplicate case)
                errorsDetected.add("Duplicate Position: \"" + color + " " + piece + " " + position + "\" - Conflicts with \"" + color + " " + expectedPiece + " " + position + "\".");
            } else {
                // Piece in an unexpected position
                validPositionsList.get(color)
                    .computeIfAbsent(piece, k -> new ArrayList<>())
                    .add(position);
            }

            occupiedPositions.put(position, color + " " + piece);
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Find missing pieces
            for (Map.Entry<String, Map<String, String>> entry : standardSetup.entrySet()) {
                String color = entry.getKey();
                for (Map.Entry<String, String> pieceEntry : entry.getValue().entrySet()) {
                    missingPieces.get(color).add("1 " + pieceEntry.getValue() + " (" + pieceEntry.getKey() + ")");
                }
            }

            // Output Missing Pieces
            context.write(new Text("Missing Pieces:"), new Text(""));
            for (String color : Arrays.asList("White", "Black")) {
                if (!missingPieces.get(color).isEmpty()) {
                    context.write(new Text("- " + color + ": " + String.join(", ", missingPieces.get(color))), new Text(""));
                }
            }

            // Output Position Validation
            context.write(new Text("Position Validation:"), new Text(""));
            context.write(new Text("- Valid Positions:"), new Text(""));
            outputValidPositions(context, "White");
            outputValidPositions(context, "Black");

            // Output Errors Detected
            context.write(new Text("Errors Detected:"), new Text(""));
            for (String error : errorsDetected) {
                context.write(new Text("- " + error), new Text(""));
            }
        }

        private void outputValidPositions(Context context, String color) throws IOException, InterruptedException {
            String[] pieceOrder = {"King", "Queen", "Rook", "Bishop", "Knight", "Pawn"};
            Map<String, List<String>> colorPieces = validPositionsList.get(color);

            for (String piece : pieceOrder) {
                List<String> positions = colorPieces.get(piece);
                if (positions != null && !positions.isEmpty()) {
                    Collections.sort(positions);
                    String positionStr = positions.size() == 1 ? "(" + positions.get(0) + ")" : 
                        (piece.equals("Pawn") ? "(" + String.join(", ", positions) + ")" : 
                        "(" + String.join("), (", positions) + ")");
                    context.write(new Text("  - " + color + " " + (piece.equals("Pawn") ? piece + "s" : piece) + " " + positionStr), new Text(""));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Chess Piece Analyzer");
        job.setJarByClass(ChessValidator.class);
        job.setMapperClass(ChessMapper.class);
        job.setReducerClass(ChessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}