import * as fuzz from 'fuzzball';

export default class SanctionsScanner {
  constructor(request) {
    this.sanctionsList = [];
    this.request = request;
    this.log = request.log;
  }

  /**
   * Set the sanctions list to use for scanning
   * @param {Array} list - Array of sanctions data to scan against
   */
  setSanctionsList(list) {
    this.sanctionsList = list;
  }

  /**
   * Calculates the match percentage between a provided name and names in sanctions list
   * @param {string} providedName - The name to check against the sanctions list
   * @param {string[]} fullNames - Array of full names from the sanctions list entry
   * @param {Object} options - Optional configuration parameters
   * @param {number} options.fuzzyThreshold - Threshold for fuzzy matching (default: 85)
   * @param {boolean} options.caseSensitive - Whether matching should be case sensitive (default: false)
   * @returns {Object} Result object with best match percentage and details
   */
  async matchName(providedName, fullNames, options = {}) {
    const matchStartTime = performance.now();

    // Default options
    const config = {
      fuzzyThreshold: 85, // Minimum fuzzy match percentage to consider
      caseSensitive: false,
      ...options
    };

    if (!providedName || !fullNames || !Array.isArray(fullNames) || fullNames.length === 0) {
      return { match: 0, details: [], matchTime: performance.now() - matchStartTime };
    }

    // Normalize the provided name
    const normalizedProvidedName = config.caseSensitive ?
      providedName.trim() :
      providedName.toLowerCase().trim();

    // Split the provided name into individual parts
    const providedNameParts = normalizedProvidedName.split(/\s+/).filter(part => part.length > 0);

    if (providedNameParts.length === 0) {
      return { match: 0, details: [], matchTime: performance.now() - matchStartTime };
    }

    let bestMatch = {
      name: '',
      overallScore: 0,
      partScores: []
    };

    // Process each full name in the array

    fullNames.forEach(fullName => {
      // Normalize full name according to case sensitivity option
      const normalizedFullName = config.caseSensitive ?
        fullName.trim() :
        fullName.toLowerCase().trim();

      // Split the full name into parts
      const fullNameParts = normalizedFullName.split(/\s+/).filter(part => part.length > 0);

      if (fullNameParts.length === 0) return;

      // Full name exact match check
      const fullNameRatio = fuzz.ratio(normalizedProvidedName, normalizedFullName);

      // Compare each part of the provided name with each part of the full name
      const partScores = [];
      let totalScore = 0;

      providedNameParts.forEach(providedPart => {
        let bestPartScore = 0;

        fullNameParts.forEach(fullPart => {
          const score = fuzz.ratio(providedPart, fullPart);
          if (score > bestPartScore) {
            bestPartScore = score;
          }
        });

        partScores.push({
          part: providedPart,
          score: bestPartScore
        });

        totalScore += bestPartScore;
      });

      // Calculate average score across all parts
      const averageScore = providedNameParts.length > 0 ?
        totalScore / providedNameParts.length : 0;

      // Calculate overall score as average of full name match and part-by-part match
      const overallScore = (fullNameRatio + averageScore) / 2;

      // Update best match if this one is better
      if (overallScore > bestMatch.overallScore) {
        bestMatch = {
          name: fullName,
          overallScore: overallScore,
          partScores: partScores,
          fullNameScore: fullNameRatio
        };
      }
    });

    const matchTime = performance.now() - matchStartTime;
    // If this is unusually slow (more than 10ms), log it for investigation
    /*if (matchTime > 10) {
      console.log(`Slow match detected: ${providedName} against ${fullNames.join(', ')} took ${matchTime.toFixed(2)}ms`);
    }*/

    return {
      match: Math.round(bestMatch.overallScore), // Round to nearest integer percentage
      bestMatchName: bestMatch.name,
      details: {
        partScores: bestMatch.partScores,
        fullNameScore: bestMatch.fullNameScore
      },
      matchTime: matchTime
    };
  }

  /**
   * Match a name against the sanctions list
   * @param {Object} req - Request object
   * @param {Object} reply - Reply object
   * @returns {Object} Match results
   */
  async match(req, reply) {
    try {
      const startTime = performance.now();
      let timings = {
        total: 0,
        processing: 0,
        matchingCount: 0,
        slowMatches: 0,
        fastestMatch: Number.MAX_VALUE,
        slowestMatch: 0,
        totalMatchTime: 0
      };

      let providedName;
      let options = {};

      // Handle both GET and POST requests
      if (req.method === 'GET') {
        providedName = req.params.id;

        // Default threshold to 75 for GET requests
        options.threshold = 75;

        // Get entity type from query parameter
        if (req.query.type) {
          options.entityType = req.query.type;
        } else {
          options.entityType = "Individual";
        }

        // Construct options object
        options = {
          fuzzyThreshold: 75,
          entityType: options.entityType,
          threshold: 75
        };
      } else {
        // Assume POST request
        providedName = req.body.name;

        // Get options from request body if provided
        if (req.body.options) {
          options = req.body.options;
        } else {
          options = {
            fuzzyThreshold: 75,
            entityType: "Individual",
            threshold: 75
          };
        }
      }

      if (!providedName) {
        return reply.status(400).send({
          error: "Name parameter is required"
        });
      }

      // Check if sanctions list is loaded
      if (!this.sanctionsList || this.sanctionsList.length === 0) {
        return reply.status(500).send({
          error: "Sanctions list not loaded",
          message: "The sanctions list must be set before matching"
        });
      }

      // Threshold for considering a match (%)
      const threshold = options.threshold || 75;

      const matches = [];

      // Process each entry in the sanctions list
      const processingStart = performance.now();

      for (const entry of this.sanctionsList) {
        // Skip entries if their source is not in the allowed sources list (if provided)
        if (options.sources && Array.isArray(options.sources) && options.sources.length > 0) {
          if (!entry.source || !options.sources.includes(entry.source)) {
            continue; // Skip this entry as its source is not in the allowed list
          }
        }

        if (entry.full_names && entry.entity_type === options.entityType) {
          const matchResult = await this.matchName(providedName, entry.full_names, options);
          timings.matchingCount++;

          // Track match timing statistics
          const matchTime = matchResult.matchTime;
          timings.totalMatchTime += matchTime;
          if (matchTime > timings.slowestMatch) timings.slowestMatch = matchTime;
          if (matchTime < timings.fastestMatch) timings.fastestMatch = matchTime;
          if (matchTime > 10) timings.slowMatches++;

          // Only include matches above the threshold
          if (matchResult.match >= threshold) {
            matches.push({
              entry: {
                reference: entry.reference,
                comments: entry.comments,
                fullNames: entry.full_names,
                entityType: entry.entity_type,
                country: entry.country,
                designation: entry.designation,
                source: entry.source
              },
              matchResult: {
                match: matchResult.match,
                bestMatchName: matchResult.bestMatchName,
                details: matchResult.details,
                matchTime: matchResult.matchTime
              }
            });
          }
        }
      }
      timings.processing = performance.now() - processingStart;

      // Sort results by match percentage in descending order
      matches.sort((a, b) => b.matchResult.match - a.matchResult.match);

      timings.total = performance.now() - startTime;
      this.log.info(`Performance: Total=${timings.total.toFixed(2)}ms, Processing=${timings.processing.toFixed(2)}ms, Matched ${timings.matchingCount} entries`);
      if (timings.slowMatches > 0) {
        this.log.info(`Slow matches: ${timings.slowMatches} matches took >10ms, slowest: ${timings.slowestMatch.toFixed(2)}ms`);
      }

      return {
        query: providedName,
        threshold: threshold,
        totalMatches: matches.length,
        matches: matches,
        performance: {
          totalTimeMs: Math.round(timings.total),
          processingMs: Math.round(timings.processing),
          entriesMatched: timings.matchingCount,
          averageMatchTimeMs: timings.matchingCount > 0 ? Math.round(timings.totalMatchTime / timings.matchingCount) : 0,
          slowMatches: timings.slowMatches,
          fastestMatchMs: Math.round(timings.fastestMatch),
          slowestMatchMs: Math.round(timings.slowestMatch)
        }
      };
    } catch (error) {
      this.log.error("Error in match function:" + error.message);
      return {
        error: "Failed to process match request",
        message: error.message
      };
    }
  }

  async internalMatch(_providedName, _options = null) {

    /// options is an object with the following properties:
    /// - fuzzyThreshold: number
    /// - entityType: string
    /// - threshold: number
    /// - sources: array of strings

    try {
      const startTime = performance.now();
      let timings = {
        total: 0,
        processing: 0,
        matchingCount: 0,
        slowMatches: 0,
        fastestMatch: Number.MAX_VALUE,
        slowestMatch: 0,
        totalMatchTime: 0
      };

      let providedName = _providedName;
      let options = _options || {};

      if (!providedName) {
        return {
          error: "Name parameter is required"
        };
      }

      // Check if sanctions list is loaded
      if (!this.sanctionsList || this.sanctionsList.length === 0) {
        return {
          error: "Sanctions list not loaded",
          message: "The sanctions list must be set before matching"
        };
      }

      // Threshold for considering a match (%)
      const threshold = options.threshold || 75;

      const matches = [];

      // Process each entry in the sanctions list
      const processingStart = performance.now();

      for (const entry of this.sanctionsList) {
        // Skip entries if their source is not in the allowed sources list (if provided)
        if (options.sources && Array.isArray(options.sources) && options.sources.length > 0) {
          if (!entry.source || !options.sources.includes(entry.source)) {
            continue; // Skip this entry as its source is not in the allowed list
          }
        }

        if (entry.full_names && entry.entity_type === options.entityType) {
          const matchResult = await this.matchName(providedName, entry.full_names, options);
          timings.matchingCount++;

          // Track match timing statistics
          const matchTime = matchResult.matchTime;
          timings.totalMatchTime += matchTime;
          if (matchTime > timings.slowestMatch) timings.slowestMatch = matchTime;
          if (matchTime < timings.fastestMatch) timings.fastestMatch = matchTime;
          if (matchTime > 10) timings.slowMatches++;

          // Only include matches above the threshold
          if (matchResult.match >= threshold) {
            matches.push({
              entry: {
                reference: entry.reference,
                comments: entry.comments,
                fullNames: entry.full_names,
                entityType: entry.entity_type,
                country: entry.country,
                listed: entry.listed,
                birth_year: entry.birth_year,
                designation: entry.designation,
                source: entry.source
              },
              matchResult: {
                match: matchResult.match,
                bestMatchName: matchResult.bestMatchName,
                details: matchResult.details,
                matchTime: matchResult.matchTime
              }
            });
          }
        }
      }
      timings.processing = performance.now() - processingStart;

      // Sort results by match percentage in descending order
      matches.sort((a, b) => b.matchResult.match - a.matchResult.match);

      timings.total = performance.now() - startTime;
      /*console.log(`Performance: Total=${timings.total.toFixed(2)}ms, Processing=${timings.processing.toFixed(2)}ms, Matched ${timings.matchingCount} entries`);
      if (timings.slowMatches > 0) {
        this.log.info(`Slow matches: ${timings.slowMatches} matches took >10ms, slowest: ${timings.slowestMatch.toFixed(2)}ms`);
      }*/

      let letTopResultForList = {};
      let letTopResultForListArray = [];

      for (let match of matches) {
        if (!letTopResultForList[match.entry.source]) {
          letTopResultForList[match.entry.source] = match;
        }
      }

      for (let source in letTopResultForList) {
        letTopResultForListArray.push(letTopResultForList[source]);
      }

      letTopResultForListArray.sort((a, b) => b.matchResult.match - a.matchResult.match);

      return {
        query: providedName,
        threshold: threshold,
        totalMatches: matches.length,
        matches: matches,
        topResults: letTopResultForListArray,
        performance: {
          totalTimeMs: Math.round(timings.total),
          processingMs: Math.round(timings.processing),
          entriesMatched: timings.matchingCount,
          averageMatchTimeMs: timings.matchingCount > 0 ? Math.round(timings.totalMatchTime / timings.matchingCount) : 0,
          slowMatches: timings.slowMatches,
          fastestMatchMs: Math.round(timings.fastestMatch),
          slowestMatchMs: Math.round(timings.slowestMatch)
        }
      };
    } catch (error) {
      this.log.error("Error in match function:" + error.message);
      return {
        error: "Failed to process match request",
        message: error.message
      };
    }
  }
}
