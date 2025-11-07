import { constants } from 'crypto';
import * as parser from 'xml2json';
import axios from 'axios';
const http = axios.default;
import https from 'https';
import fs from 'fs';
const fsPromises = fs.promises;
import crypto from 'crypto';
import chokidar from 'chokidar';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { readFile } from 'fs/promises';
import path from 'path';

// Get the directory name in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export default class SanctionListManager {
  constructor(redis, db, log ) {
    this.redis = redis;
    this.db = db;
    this.log = log;
    this.downloadLinks = {
      "s26A": {
        url: "https://transfer.fic.gov.za/public/folder/XNK-dueZDUma9kOqjPWrAA/Downloads/Consolidated%20United%20Nations%20Security%20Council%20Sanctions%20List.xml",
        options: {
          rejectUnauthorized: false,
        }
      },
      "s28A": {
        url: "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
        options: {
          rejectUnauthorized: false,
          secureOptions: constants.SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION
        }
      },
      "EUFinancialSanctions": {
        url: "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList/content?token=dG9rZW4tMjAxNw",
        options: {
          rejectUnauthorized: false,
        }
      },
      "OFAC": {
        url: "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ENHANCED.XML",
        options: {
          rejectUnauthorized: false,
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/xml, text/xml, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br'
          }
        }
      }
    };

    this.fileWatcher = null;
  }

  calculateBirthYear(birthDate) {
    let date = new Date(birthDate);
    return date.getFullYear();
  }

  hashList(jsonArray) {
    let hash = '';
    for (let item of jsonArray) {
      hash = JSON.stringify(item);
      item.md5 = crypto.createHash('md5').update(hash).digest("hex");
    }
    return jsonArray;
  }

  compareLists(downloaded, saved) {
    let notFound = [];

    for (let item of downloaded) {
      if (!saved.find(i => i.md5 == item.md5)) {
        notFound.push(item);
      }
    }

    return { notFound };
  }

  async parseS26A(json) {
    let result = [];
    /* parse natural persons */
    json.NewDataSet.Table.map((value) => {
      let fullNames = [];
      fullNames.push(value.FullName);
      if (value.IndividualAlias) {
        fullNames.push(value.IndividualAlias.split(',')[1].trim());
      }

      let birthYear = this.calculateBirthYear(value.IndividualDateOfBirth);

      result.push({
        reference: value.ReferenceNumber,
        full_names: fullNames,
        entity_type: value.EntityAddress ? 'Institution' : 'Individual',
        country: value.Nationality,
        designation: value.Designation,
        comments: value.Comments,
        listed: new Date(value.ListedOn).getTime(),
        source: "s26A",
        birth_year: birthYear,
      });
    });

    /* parse institutions */
    if (json.NewDataSet.Table1) {
      json.NewDataSet.Table1.map((value) => {
        let fullNames = [];
        fullNames.push(value.FirstName);

        let birthYear = this.calculateBirthYear(value.IndividualDateOfBirth);

        result.push({
          reference: value.ReferenceNumber,
          full_names: fullNames,
          entity_type: value.EntityAddress ? 'Institution' : 'Individual',
          country: value.Nationality,
          comments: value.Comments,
          listed: new Date(value.ListedDate).getTime(),
          source: "s26A",
          birth_year: birthYear,
        });
      });
    }

    return this.hashList(result);
  }

  async parseS28A(json) {
    let result = [];
    /* parse natural persons */
    json.CONSOLIDATED_LIST.INDIVIDUALS.INDIVIDUAL.map((value) => {
      let fullNames = [];
      fullNames.push(value.FIRST_NAME + ' ' + value.SECOND_NAME);
      if (Array.isArray(value.INDIVIDUAL_ALIAS)) {
        for (let alias of value.INDIVIDUAL_ALIAS) {
          if (alias.QUALITY == 'Good') {
            fullNames.push(alias.ALIAS_NAME);
          }
        }
      }

      let birthYear = null;
      let designation = null;

      if (value.INDIVIDUAL_DATE_OF_BIRTH) {
        if (Array.isArray(value.INDIVIDUAL_DATE_OF_BIRTH)) {
          birthYear = this.calculateBirthYear(value.INDIVIDUAL_DATE_OF_BIRTH[0].VALUE);
        } else {
          birthYear = this.calculateBirthYear(value.INDIVIDUAL_DATE_OF_BIRTH.VALUE);
        }
      }

      if (value.DESIGNATION) {
        if (Array.isArray(value.DESIGNATION.VALUE)) {
          designation = value.DESIGNATION.VALUE.join('\n');
        } else {
          designation = value.DESIGNATION.VALUE;
        }
      }

      result.push({
        reference: value.REFERENCE_NUMBER,
        full_names: fullNames,
        entity_type: 'Individual',
        country: value.NATIONALITY ? value.NATIONALITY.VALUE : '',
        comments: value.COMMENTS1,
        designation: designation,
        listed: new Date(value.LISTED_ON).getTime(),
        source: "s28A",
        birth_year: birthYear,
      });
    });

    /* parse institutions */
    if (json.CONSOLIDATED_LIST.ENTITIES.ENTITY) {
      json.CONSOLIDATED_LIST.ENTITIES.ENTITY.map((value) => {
        let fullNames = [];
        fullNames.push(value.FIRST_NAME);

        if (Array.isArray(value.ENTITY_ALIAS)) {
          for (let alias of value.ENTITY_ALIAS) {
            fullNames.push(alias.ALIAS_NAME);
          }
        }

        let birthYear = null;

        result.push({
          reference: value.REFERENCE_NUMBER,
          full_names: fullNames,
          entity_type: 'Institution',
          country: value.ENTITY_ADDRESS ? value.ENTITY_ADDRESS.COUNTRY : '',
          comments: value.COMMENTS1,
          listed: new Date(value.LISTED_ON).getTime(),
          source: "s28A",
          birth_year: birthYear,
        });
      });
    }

    return this.hashList(result);
  }

  formatName(name) {
    if (!name) return '';

    try {
      // Replace commas with spaces
      name = name.replace(/,/g, ' ');

      // Replace multiple spaces with a single space
      name = name.replace(/\s+/g, ' ');

      // Trim leading and trailing spaces
      name = name.trim();

      return name;
    } catch (e) {
      this.log.error(name, "error", e.message);
      return '';
    }

  }

  async parseEUFinancialSanctions(json) {
    this.log.info("Processing EU Financial Sanctions list data");   
    var list = [];

    if (!json) {
      throw new Error("No data received from EU Financial Sanctions API");
    }

    try {
      // Check if we have the expected structure
      if (!json || !json.export) {
        return list;
      }

      // Check if we have sanctionEntity in the export
      if (!json.export.sanctionEntity) {
        return list;
      }

      // Ensure sanctionEntity is an array
      const entities = Array.isArray(json.export.sanctionEntity) ? json.export.sanctionEntity : [json.export.sanctionEntity];

      entities.forEach(entity => {
        try {
          // Entity fields
          const entityType = entity.subjectType?.code || '';
          const classificationCode = entity.subjectType?.classificationCode || '';
          const logicalId = entity.logicalId || '';
          const remark = entity.remark || '';

          // Regulation array
          const regulation = entity.regulation ? {
            regulationType: entity.regulation.regulationType || '',
            publicationDate: entity.regulation.publicationDate || '',
            numberTitle: entity.regulation.numberTitle || '',
            programme: entity.regulation.programme || ''
          } : {};

          // Array to store all full names including aliases
          let fullNames = [];

          // Process name aliases array
          if (entity.nameAlias) {
            const nameAliases = Array.isArray(entity.nameAlias) ? entity.nameAlias : [entity.nameAlias];
            nameAliases.forEach(nameObj => {
              const firstName = nameObj.firstName || '';
              const middleName = nameObj.middleName || '';
              const lastName = nameObj.lastName || '';
              const wholeName = nameObj.wholeName || '';
              const nameLogicalId = nameObj.logicalId || '';
              const nameFunction = nameObj.function || '';

              // Construct full name with all name parts
              let fullName = '';

              // If wholeName is provided, use it
              if (wholeName) {
                fullName = wholeName;
              } else {
                // Otherwise construct from parts
                fullName = [firstName, middleName, lastName]
                  .filter(part => part && typeof part === 'string' && part.trim() !== '')
                  .join(' ');
              }

              // Format the name: remove commas and ensure spaces between words
              fullName = this.formatName(fullName);

              // Add the full name to the array if it's not empty and not already included
              if (fullName && !fullNames.includes(fullName)) {
                fullNames.push(fullName);
              }
            });
          }

          // Get country information
          const country = entity.citizenship?.countryDescription || '';
          const countryCode = entity.citizenship?.countryIso2Code || '';

          // Get birth information
          let birthYear = entity.birthdate && entity.birthdate[0]?.year || '';
          birthYear = birthYear.length > 0 ? parseInt(birthYear) : null;
          const birthPlace = entity.birthdate && entity.birthdate[0]?.city || '';

          // Create a record for this entity
          const record = {
            reference: logicalId,
            full_names: fullNames, // Array of all full names including aliases
            country: country,
            birth_year: birthYear,
            entity_type: entityType === 'person' ? 'Individual' : 'Institution',
            comments: remark,
            listed: new Date(regulation.publicationDate).getTime(),
            source: 'EUFinancialSanctions'
          };

          list.push(record);
        } catch (entityError) {
          console.error("Error processing EU Financial Sanctions entity:", entityError);
        }
      });
    } catch (error) {
      console.error("Error processing EU Financial Sanctions data:", error);
      throw error;
    }

    return this.hashList(list);
  }

  parseOFAC(json) {
    var list = [];

    if (!json) {
      throw new Error("No data received from OFAC API");
    }

    try {
      if (json.sanctionsData && json.sanctionsData.entities && json.sanctionsData.entities.entity) {
        const entities = Array.isArray(json.sanctionsData.entities.entity)
          ? json.sanctionsData.entities.entity
          : [json.sanctionsData.entities.entity];

        entities.forEach(entity => {
          try {
            // Get basic entity information
            const entityId = entity.id || '';

            // Get entity type
            let entityType = 'Unknown';
            if (entity.generalInfo && entity.generalInfo.entityType && entity.generalInfo.entityType.$t) {
              entityType = entity.generalInfo.entityType.$t === 'Individual' ? 'Individual' : 'Institution';
            }

            // Array to store all full names including aliases
            let fullNames = [];

            // Process all names (both primary and aliases)
            if (entity.names && entity.names.name) {
              const namesArray = Array.isArray(entity.names.name) ? entity.names.name : [entity.names.name];

              namesArray.forEach(name => {
                if (name.translations && name.translations.translation) {
                  const translation = name.translations.translation;

                  // Get the full name from the translation
                  let fullName = '';

                  // For individuals, we might have first/last name
                  if (entityType === 'Individual' && translation.formattedFirstName) {
                    // Get all name parts
                    const firstName = translation.formattedFirstName || '';
                    const lastName = translation.formattedLastName || '';

                    // Construct full name with all name parts
                    fullName = [firstName, lastName]
                      .filter(part => part && typeof part === 'string' && part.trim() !== '')
                      .join(' ');
                  } else {
                    // For entities or when there's no specific first/last name
                    fullName = translation.formattedFullName || translation.formattedLastName || '';
                  }

                  // If no formatted full name, try to construct from name parts
                  if (!fullName && translation.nameParts && translation.nameParts.namePart) {
                    const nameParts = Array.isArray(translation.nameParts.namePart) ?
                      translation.nameParts.namePart : [translation.nameParts.namePart];

                    // Extract all name parts in order
                    const nameValues = nameParts.map(part => part.value);

                    // Construct full name with all name parts
                    fullName = nameValues.join(' ');
                  }

                  // Format the name: remove commas and ensure spaces between words
                  fullName = this.formatName(fullName);

                  // Add the full name to the array if it's not empty and not already included
                  if (fullName && !fullNames.includes(fullName)) {
                    fullNames.push(fullName);
                  }
                }
              });
            }

            // Process aliases if they exist
            if (entity.aliases && entity.aliases.alias) {
              const aliasArray = Array.isArray(entity.aliases.alias) ? entity.aliases.alias : [entity.aliases.alias];

              aliasArray.forEach(alias => {
                if (alias.translations && alias.translations.translation) {
                  const translation = alias.translations.translation;

                  // Get the alias name from the translation
                  let aliasName = '';

                  // For individuals, we might have first/last name
                  if (entityType === 'Individual' && translation.formattedFirstName) {
                    // Get all name parts
                    const firstName = translation.formattedFirstName || '';
                    const lastName = translation.formattedLastName || '';

                    // Construct alias name with all name parts
                    aliasName = [firstName, lastName]
                      .filter(part => part && typeof part === 'string' && part.trim() !== '')
                      .join(' ');
                  } else {
                    // For entities or when there's no specific first/last name
                    aliasName = translation.formattedFullName || translation.formattedLastName || '';
                  }

                  // If no formatted full name, try to construct from name parts
                  if (!aliasName && translation.nameParts && translation.nameParts.namePart) {
                    const nameParts = Array.isArray(translation.nameParts.namePart) ?
                      translation.nameParts.namePart : [translation.nameParts.namePart];

                    // Extract all name parts in order
                    const nameValues = nameParts.map(part => part.value);

                    // Construct alias name with all name parts
                    aliasName = nameValues.join(' ');
                  }

                  // Format the alias name: remove commas and ensure spaces between words
                  aliasName = this.formatName(aliasName);

                  // Add the alias name to the array if it's not empty and not already included
                  if (aliasName && !fullNames.includes(aliasName)) {
                    fullNames.push(aliasName);
                  }
                }
              });
            }

            // Get country information
            let country = '';
            let cityOrAddress = '';

            if (entity.addresses && entity.addresses.address) {
              const addressArray = Array.isArray(entity.addresses.address) ?
                entity.addresses.address : [entity.addresses.address];

              // Use the first address
              const address = addressArray[0];

              if (address.country && address.country.$t) {
                country = address.country.$t;
              }

              // Try to get city or other address parts
              if (address.translations && address.translations.translation &&
                address.translations.translation.addressParts &&
                address.translations.translation.addressParts.addressPart) {

                const addressParts = Array.isArray(address.translations.translation.addressParts.addressPart) ?
                  address.translations.translation.addressParts.addressPart :
                  [address.translations.translation.addressParts.addressPart];

                // Join address parts or find city part
                const cityPart = addressParts.find(part =>
                  part.type && part.type.$t === 'CITY');

                if (cityPart) {
                  cityOrAddress = cityPart.value;
                } else {
                  cityOrAddress = addressParts.map(part => part.value).join(', ');
                }
              }
            }

            // Create an object to store remarks information
            let remarksObj = {};

            // Get title information
            if (entity.generalInfo && entity.generalInfo.title && entity.generalInfo.title.$t) {
              remarksObj["Title"] = entity.generalInfo.title.$t;
            }

            // Get sanctions list information
            if (entity.sanctionsLists && entity.sanctionsLists.sanctionsList) {
              const sanctionsListArray = Array.isArray(entity.sanctionsLists.sanctionsList)
                ? entity.sanctionsLists.sanctionsList
                : [entity.sanctionsLists.sanctionsList];

              sanctionsListArray.forEach(list => {
                if (list.$t) {
                  const datePublished = list.datePublished ? ` (Published: ${list.datePublished})` : '';
                  remarksObj["SanctionsList"] = `${list.$t}${datePublished}`;
                }
              });
            }

            // Get sanctions program information
            if (entity.sanctionsPrograms && entity.sanctionsPrograms.sanctionsProgram) {
              const sanctionsProgramArray = Array.isArray(entity.sanctionsPrograms.sanctionsProgram)
                ? entity.sanctionsPrograms.sanctionsProgram
                : [entity.sanctionsPrograms.sanctionsProgram];

              sanctionsProgramArray.forEach(program => {
                if (program.$t) {
                  remarksObj["SanctionsProgram"] = program.$t;
                }
              });
            }

            // Get sanctions type information
            if (entity.sanctionsTypes && entity.sanctionsTypes.sanctionsType) {
              const sanctionsTypeArray = Array.isArray(entity.sanctionsTypes.sanctionsType)
                ? entity.sanctionsTypes.sanctionsType
                : [entity.sanctionsTypes.sanctionsType];

              sanctionsTypeArray.forEach(type => {
                if (type.$t) {
                  remarksObj["SanctionsType"] = type.$t;
                }
              });
            }

            // Get legal authority information
            if (entity.legalAuthorities && entity.legalAuthorities.legalAuthority) {
              const legalAuthorityArray = Array.isArray(entity.legalAuthorities.legalAuthority)
                ? entity.legalAuthorities.legalAuthority
                : [entity.legalAuthorities.legalAuthority];

              legalAuthorityArray.forEach(authority => {
                if (authority.$t) {
                  remarksObj["LegalAuthority"] = authority.$t;
                }
              });
            }

            // Variables to store birthday and date added information
            let birthday = '';
            let dateAdded = '';

            // Get birthdate information
            if (entity.features && entity.features.feature) {
              const featureArray = Array.isArray(entity.features.feature)
                ? entity.features.feature
                : [entity.features.feature];

              featureArray.forEach(feature => {
                if (feature.type && feature.type.$t === 'Birthdate' && feature.value) {
                  birthday = feature.value;
                  // Birthdate information is not added to remarks

                  // Add date range information if available
                  if (feature.valueDate) {
                    const fromDate = feature.valueDate.fromDateBegin || '';
                    const toDate = feature.valueDate.toDateBegin || '';
                    const isApproximate = feature.valueDate.isApproximate === 'true' ? ' (Approximate)' : '';

                    if (fromDate && toDate && fromDate === toDate) {
                      // Birthdate range information is not added to remarks
                    } else if (fromDate && toDate) {
                      // Birthdate range information is not added to remarks
                    }
                  }
                }
              });
            }

            // Get date added information from sanctions list
            if (entity.sanctionsLists && entity.sanctionsLists.sanctionsList) {
              const sanctionsListArray = Array.isArray(entity.sanctionsLists.sanctionsList)
                ? entity.sanctionsLists.sanctionsList
                : [entity.sanctionsLists.sanctionsList];

              if (sanctionsListArray.length > 0 && sanctionsListArray[0].datePublished) {
                dateAdded = sanctionsListArray[0].datePublished;
                // Date added information is not added to remarks
              }
            }

            // Format remarks in a more readable way
            let remarksStr = '';
            for (const key in remarksObj) {
              if (remarksStr) remarksStr += ' | ';
              remarksStr += `${key}: ${remarksObj[key]}`;
            }

            const record = {
              reference: entityId,
              full_names: fullNames, // Array of all full names including aliases
              country: country,
              entity_type: entityType,
              comments: remarksStr, // More readable format
              birth_year: this.calculateBirthYear(birthday), // Add birthday as a separate field
              listed: new Date(dateAdded).getTime(), // Add date added as a separate field
              source: 'OFAC'
            };

            list.push(record);
          } catch (entityError) {
            console.error("Error processing OFAC entity:", entityError);
          }
        });
      }
    } catch (error) {
      console.error("Error processing OFAC data:", error);
      throw error;
    }

    return this.hashList(list);
  }

  async parseSanctionList(source, data) {
    switch (source) {
      case "s26A":
        return this.parseS26A(data);
      case "s28A":
        return this.parseS28A(data);
      case "EUFinancialSanctions":
        return this.parseEUFinancialSanctions(data);
      case "OFAC":
        return this.parseOFAC(data);
    }
  }

  /**
   * Get combined data from all processed JSON files
   * @returns {Promise<Array>} Combined array of all sanctions data
   */
  async getCombinedSanctionsList() {
    this.log.info('Loading and combining all processed sanction lists...');

    const lists = {};
    let combinedList = [];

    try {
      // Get all processed.json files
      const sourceKeys = Object.keys(this.downloadLinks);

      for (const source of sourceKeys) {
        const filePath = path.join(__dirname, `${source}.processed.json`);

        try {
          // Check if the file exists
          await fsPromises.access(filePath, fs.constants.F_OK);

          // Read the file
          const data = await fsPromises.readFile(filePath, 'utf8');
          const parsedData = JSON.parse(data);

          // Add to combined list
          combinedList = combinedList.concat(parsedData);

          // Store in lists object for reference
          lists[source] = parsedData;

          this.log.info(`Loaded ${parsedData.length} entries from ${filePath}`);
        } catch (err) {
          this.log.error(`File ${filePath} does not exist or could not be read`);
        }
      }

      this.log.info(`Combined list contains ${combinedList.length} total entries`);
      return combinedList;
    } catch (error) {
      this.log.error('Error combining sanction lists:', error);
      throw error;
    }
  }

  async parseToJSON(data) {
    let json = {};
    let parsed = "";
    try {
      try {
        parsed = parser.toJson(data);
      } catch (error) {
        this.log.error("Error parsing source data:", error.message);
        return null;
      }

      json = JSON.parse(parsed);
    } catch (error) {
      this.log.error("Error parsing source data:", error.message);
      return null;
    }

    return json;
  }

  async downloadLists() {
    console.time('downloadLists - Total Time');

    for (let list of Object.keys(this.downloadLinks)) {
      try {
        console.time(`downloadLists - ${list}`);
        this.log.info(`${list} downloading...`);
        let agent = new https.Agent(this.downloadLinks[list].options);
        let response = await http.get(this.downloadLinks[list].url, { httpsAgent: agent });
        let source = await this.parseToJSON(response.data);
        if (source) {
          let data = await this.parseSanctionList(list, source);
          if (data) {
            this.log.info(`${list} parsed`);
            const processedFilePath = path.join(__dirname, `${list}.processed.json`);
            const xmlFilePath = path.join(__dirname, `${list}.xml`);
            const sourceJsonFilePath = path.join(__dirname, `${list}.source.json`);

            fs.writeFileSync(processedFilePath, JSON.stringify(data, null, 2));
            fs.writeFileSync(xmlFilePath, response.data);
            fs.writeFileSync(sourceJsonFilePath, JSON.stringify(source, null, 2));
          }
        } else {
          this.log.error(`${list} no data`);
        }
        console.timeEnd(`downloadLists - ${list}`);
      } catch (e) {
        this.log.error(`${list} error: ${e.message}`);
      }


    }

    console.timeEnd('downloadLists - Total Time');
  }

  monitorProcessedFiles(onChange = null) {
    this.log.info('Starting to monitor .processed.json files for changes...');

    const watchPattern = path.join(__dirname, '*.processed.json');

    // Initialize watcher
    this.fileWatcher = chokidar.watch(watchPattern, {
      persistent: true,
      ignoreInitial: false,
      awaitWriteFinish: {
        stabilityThreshold: 2000,
        pollInterval: 100
      }
    });

    // Add event listeners
    this.fileWatcher
      .on('add', async path => {
        /*console.log(`File ${path} has been added`);
        if (typeof onChange === 'function') {
          const combinedList = await this.getCombinedSanctionsList();
          onChange(combinedList, 'add', path);
        }*/
      })
      .on('change', async path => {
        this.log.info(`File ${path} has been changed`);
        if (typeof onChange === 'function') {
          const combinedList = await this.getCombinedSanctionsList();
          onChange(combinedList, 'change', path);
        }
      })
      .on('unlink', async path => {
        /*console.log(`File ${path} has been removed`);
        if (typeof onChange === 'function') {
          const combinedList = await this.getCombinedSanctionsList();
          onChange(combinedList, 'unlink', path);
        }*/
      });

    return this.fileWatcher;
  }

  async start(monitorFiles = true, monitorDuration = 60000, onChange = null) {
    try {
      console.time('Total Execution Time');

      // Start downloading lists
      await this.downloadLists();

      // Start file monitoring if requested
      if (monitorFiles) {
        const fileWatcher = this.monitorProcessedFiles(onChange);

        // Wait for the specified duration
        this.log.info(`File watcher will run for ${monitorDuration / 1000} seconds and then stop...`);
        await new Promise(resolve => setTimeout(resolve, monitorDuration));

        // Safely close the file watcher
        await fileWatcher.close();
        this.log.info('File watcher closed successfully');
      }

      console.timeEnd('Total Execution Time');
    } catch (error) {
      this.log.error('Error in SanctionListManager:', error);
    }
  }

  async trackEntries(entries) {
    const startTime = performance.now();
    let successCount = 0;
    let errorCount = 0;
    let batchSize = 100; // Process entries in batches of 100

    let redis = this.redis;

    if (redis) {
      try {
        // Process entries in batches to minimize network overhead
        for (let i = 0; i < entries.length; i += batchSize) {
          const batch = entries.slice(i, i + batchSize);
          const pipeline = redis.pipeline(); // ioredis pipeline

          for (let entry of batch) {
            if (entry.md5) {
              // Queue commands in pipeline with ioredis syntax
              pipeline.set(`sanctionlist:${entry.md5}`, JSON.stringify(entry.md5), 'EX', 60 * 60 * 24 * 30);
              pipeline.sadd('sanctionlist:md5s', entry.md5); // Store just the md5 hash, not the full key
            }
          }

          // Execute all commands in the pipeline at once
          const results = await pipeline.exec();

          // Count successes and errors from results
          if (results) {
            for (const [index, result] of results.entries()) {
              const [err, _] = result;
              if (err) {
                errorCount++;
              } else {
                // Count one success per entry (two commands per entry)
                if (index % 2 === 0) {
                  successCount++;
                }
              }
            }
          }
        }
      } catch (error) {
        console.error(`Error in trackEntries batch processing:`, error.message);
        errorCount += batchSize; // Approximate error count for batched operations
      }
    } else {
      console.error("Redis client not found");
    }

    const endTime = performance.now();
    const executionTime = endTime - startTime;

    console.log(`trackEntries performance: Total=${executionTime.toFixed(2)}ms, Entries=${entries.length}, Success=${successCount}, Errors=${errorCount}`);

    return {
      executionTimeMs: Math.round(executionTime),
      totalEntries: entries.length,
      successCount: successCount,
      errorCount: errorCount
    };
  }

  /**
   * Removes all sanctionList entries from Redis and deletes the set
   * @returns {Object} Stats about the removal operation
   */
  async clearSanctionListEntries() {
    const startTime = performance.now();
    let removedCount = 0;
    let errors = [];
    let totalEntries = 0;
    let batchSize = 100; // Process deletions in batches

    let redis = this.redis;

    if (redis) {
      try {
        // Collect all keys first using ioredis sscan
        const keys = [];
        let cursor = '0';
        do {
          // ioredis sscan returns [cursor, members] array
          const reply = await redis.sscan('sanctionlist:md5s', cursor, 'COUNT', 100);
          cursor = reply[0];
          const members = reply[1];

          for (const md5 of members) {
            keys.push(md5);
            totalEntries++;
          }
        } while (cursor !== '0');

        // Process deletions in batches
        for (let i = 0; i < keys.length; i += batchSize) {
          const batch = keys.slice(i, i + batchSize);
          const pipeline = redis.pipeline();

          // Queue all delete operations in the pipeline
          for (const md5 of batch) {
            pipeline.del(`sanctionlist:${md5}`);
          }

          // Execute the pipeline
          const results = await pipeline.exec();

          // Process results
          if (results) {
            for (const [index, result] of results.entries()) {
              const [err, _] = result;
              if (err) {
                const md5 = batch[index];
                console.error(`Error removing entry ${md5}:`, err.message);
                errors.push({ md5, error: err.message });
              } else {
                removedCount++;
              }
            }
          }
        }

        // Delete the set itself
        await redis.del('sanctionlist:md5s');

        const endTime = performance.now();
        const executionTime = endTime - startTime;

        console.log(`clearSanctionListEntries performance: Total=${executionTime.toFixed(2)}ms, Removed=${removedCount}/${totalEntries} entries`);

        return {
          success: true,
          executionTimeMs: Math.round(executionTime),
          totalEntries,
          removedCount,
          errors: errors.length > 0 ? errors : null
        };
      } catch (error) {
        console.error("Error in clearSanctionListEntries:", error);
        return {
          success: false,
          error: error.message
        };
      }
    } else {
      console.error("Redis client not found");
      return {
        success: false,
        error: "Redis client not found"
      };
    }
  }

  /**
   * Filters a list of entries and returns only those whose MD5 hashes don't exist in Redis cache
   * @param {Array} entries - Array of entries containing md5 properties
   * @returns {Object} Result containing new filtered list and stats
   */
  async filterNewEntries(entries) {
    const startTime = performance.now();
    const newEntries = [];
    let processedCount = 0;
    let existingCount = 0;
    let errorCount = 0;
    let batchSize = 100; // Process entries in batches of 100

    let redis = this.redis;

    if (!redis) {
      console.error("Redis client not found");
      return {
        success: false,
        error: "Redis client not found"
      };
    }

    if (!entries || !Array.isArray(entries) || entries.length === 0) {
      return {
        success: true,
        totalEntries: 0,
        newEntries: [],
        existingCount: 0,
        executionTimeMs: 0
      };
    }

    try {
      // Process entries in batches to minimize network overhead
      for (let i = 0; i < entries.length; i += batchSize) {
        const batch = entries.slice(i, i + batchSize);
        const pipeline = redis.pipeline();

        // Queue exists checks in pipeline
        for (const entry of batch) {
          if (entry.md5) {
            pipeline.exists(`sanctionlist:${entry.md5}`);
          }
        }

        // Execute pipeline
        const results = await pipeline.exec();

        // Process results
        if (results) {
          for (let j = 0; j < results.length; j++) {
            const [err, exists] = results[j];
            const entry = batch[j];

            if (err) {
              console.error(`Error checking entry ${entry.md5}:`, err.message);
              errorCount++;
            } else {
              processedCount++;
              if (exists === 1) {
                // Entry exists in Redis
                existingCount++;
              } else {
                // Entry doesn't exist, add to new entries list
                newEntries.push(entry);
              }
            }
          }
        }
      }

      const endTime = performance.now();
      const executionTime = endTime - startTime;

      console.log(`filterNewEntries performance: Total=${executionTime.toFixed(2)}ms, Processed=${processedCount}, New=${newEntries.length}, Existing=${existingCount}`);

      return {
        success: true,
        executionTimeMs: Math.round(executionTime),
        totalEntries: entries.length,
        newEntries: newEntries,
        existingCount: existingCount,
        errorCount: errorCount
      };
    } catch (error) {
      console.error("Error in filterNewEntries:", error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  async enabledAccountableInstitutions(screeningMustBeEnabled = false) {
    if(screeningMustBeEnabled) {
      let institutions = await this.db.collection('AccountableInstitution').find({ enabledScreening : true }).project({ "enabledScreening":1, "regName" : 1, "remedialActionUsers" : 1 }).toArray();
      return institutions;
    } else {
      let institutions = await this.db.collection('AccountableInstitution').find({}).project({ "enabledScreening":1, "regName" : 1, "remedialActionUsers" : 1 }).toArray();
      return institutions;
    }
  }
}
