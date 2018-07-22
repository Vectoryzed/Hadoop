ratings = LOAD '/user/maria_dev/ml -100k/u.data' AS (userid:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage ('1')
           AS (movieID: Int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdblink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

groupedRatings = GROUP ratings By movieID;

averageRatings = FOREACH groupedRatings GENERATE group AS movieID, 
				 AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS numRatings; 

badMovies = FILTER averageRatings BY avgRating < 2.0; 

namedBadMovies - JOIN badMovies By movieID, nameLookup BY movieID;

finalResults = FOREACH namedBadMovies GENERATE nameLookup::movieTitle AS movieName, 
               badMovies :: avgRating AS avgRating, badMovies:: numRatings As numRatings;

finalResultsSorted = ORDER finalResults by numRatings DESC; 

DUMP finalResultsSorted;
