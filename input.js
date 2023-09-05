const {
  VOLUNTEER_ROLE_FACILITY,
  VOLUNTEER_ROLE_ZONE,
  VOLUNTEER_ROLE_REGION,
  VOLUNTEER_STAGES,
  MODULE_FACILITIES,
  createSecurityArray,
  DEFAULT_RETURNED,
} = require('./volunteer.constants')
const Volunteer = require('./volunteers.model')
const Congregation = require('../congregations/congregations.model')
const mongoCRUD = require('../../database/mongo')
const Document = require('../document/documents.model')
const Regions = require('../regions/regions.model')
const VolunteerValidation = require('./volunteers.validation')
const InmatesController = require('../inmates/inmates.controller')
// eslint-disable-next-line no-unused-vars
const FacilitiesController = require('../facilities/facilities.controller')
const Facilities = require('../facilities/facilities.model')
const Inmates = require('../inmates/inmates.model')
const Meetings = require('../meetings/meetings.controller')
const Study = require('../study/studies.model')
const Zone = require('../zones/zones.model')
const { S3Connector } = require('../../services')
const {
  getUtcDateNow,
  addPagination,
  uniqueIdentifier,
  sanitize,
  orNull,
  isNullOrEmptyString,
  createAddFields,
  createHash,
  randomUniqueIndentifier,
  areStageColumnsProvided,
  hasNestedCriterion,
  // eslint-disable-next-line no-unused-vars
  buildSort,
  buildPagination,
  buildProjection,
  buildCount,
  buildResponse,
  isEmptyObject,
} = require('../../utilities')
const { EmailService } = require('../../services')
const { FACILITIES_STAGES } = require('../facilities/facilities.constants')
const { addMatch, createMatch } = require('../criterion/helpers')

const s3Connection = new S3Connector()

const volunteerPropsDefaults = {
  userID: null,
  username: null,
  password: null,
  email: null,
  firstName: null,
  lastName: null,
  address: null,
  city: null,
  state: null,
  zipCode: null,
  phone: null,
  homePhone: null,
  region: null,
  lastAccess: null,
  tbTestingDate: null,
  status: null,
  role: null,
  volunteerGender: null,
  congregationID: null,
  emergencyContact: null,
  emergencyContactRelationship: null,
  emergencyContactNumber: null,
  applicantVolunteer: null,
  language: [],
  uiLanguage: 'en',
  birthDate: null,
  baptismDate: null,
  maritalStatus: null,
  appointment: [],
  applicantID: null,
  facilities: [],
  timeOff: [],
  recurringAccess: [],
  textOptIn: false,
  communicationInfo: null,
  serviceProviderID: null,
  photoLink: null,
  security: [],
  isICLWContact: false,
  isICLWVolunteer: false,
  approvedDate: null,
  approvedByUserID: null,
  created: null,
  createdBy: null,
  modified: null,
  modifiedBy: null,
  deleted: false,
  deletedDate: null,
  deletedBy: null,
  isTester: false,
  isAdmin: false,
  isBranchRep: false,
}

const facilityDefaults = {
  gender: null,
  badgeExpiration: null,
  assignments: {},
}

const columnMap = {
  name: {
    stage: VOLUNTEER_STAGES.VOLUNTEER,
    query: {
      $concat: [
        { $ifNull: [`$lastName`, ''] },
        {
          $cond: {
            if: { $ifNull: [`$firstName`, false] },
            then: ', ',
            else: '',
          },
        },
        { $ifNull: [`$firstName`, ''] },
      ],
    },
  },
  lastWelcomeEmailDate: {
    stage: VOLUNTEER_STAGES.VOLUNTEER,
    query: '$approvedDate',
  },
  isAllowedInterest: {
    stage: VOLUNTEER_STAGES.VOLUNTEER,
    query: { $ifNull: ['$isAllowedInterest', false] },
  },
}

exports.getUserSecurityMatrixByUserID = async function (userID) {
  const matchPipeline = {
    $match: {
      userID: userID,
    },
  }
  return await getUserSecurityMatrix(matchPipeline)
}

exports.getUserSecurityMatrixByEmail = async function (email) {
  const matchPipeline = {
    $match: {
      email: email,
    },
  }
  return await getUserSecurityMatrix(matchPipeline)
}

async function getUserSecurityMatrix(matchPipeline) {
  const pipeline = [
    matchPipeline,
    {
      $project: {
        _id: 0,
        userID: 1,
        username: 1, // use this for createdby/modifiedby first
        email: 1, // use this for createdby/modifiedby as a fallback if username is undefined/null
        region: 1, // added region for level determinations on 'regional'
        firstName: 1, // added to allow sending email without having to re-lookup the user
        lastName: 1, // added to allow sending email without having to re-lookup the user
        role: 1, //  added to provide role to volunteer profile tabs
        congregationID: 1,
        assignedFacilities: '$facilities.facilityID',
        facilityAssignments: '$facilities.assignments',
        'security.module': 1,
        'security.access': 1,
        'security.level': 1,
        isBranchRep: 1, //added to support branch access to volunteer and inmate data
        isICLWContact: 1,
        isICLWVolunteer: 1,
        isAllowedInterest: 1,
      },
    },
  ]

  const info = await mongoCRUD.queryCollection(Volunteer, pipeline)

  if (info.length === 0) {
    return null
  }

  const thisVolunteer = info[0]

  if (thisVolunteer.congregationID && thisVolunteer.congregationID !== '') {
    const congregationPipeline = [
      {
        $match: {
          congregationID: thisVolunteer.congregationID,
        },
      },
      {
        $unwind: {
          path: '$zones',
          preserveNullAndEmptyArrays: true,
        },
      },
      {
        $lookup: {
          from: 'zones',
          localField: 'zones.zoneID',
          foreignField: 'zoneID',
          as: 'zone',
        },
      },
      {
        $unwind: {
          path: '$zone',
        },
      },
      {
        $project: {
          _id: 0,
          zoneID: '$zone.zoneID',
          zoneName: '$zone.zoneName',
        },
      },
    ]
    const zones = await mongoCRUD.queryCollection(
      Congregation,
      congregationPipeline,
    )
    thisVolunteer.zones = zones ? zones : []
  }
  const oversightPipeline = [
    {
      $match: {
        $or: [
          { assistantContacts: thisVolunteer.userID },
          { contactID: thisVolunteer.userID },
        ],
      },
    },
    {
      $unwind: {
        path: '$zoneTerritories',
      },
    },
    {
      $project: {
        _id: 0,
        zoneID: 1,
        zoneName: 1,
        territoryID: '$zoneTerritories.territoryID',
        territory: '$zoneTerritories.name',
        territoryAbbreviation: '$zoneTerritories.abbreviation',
      },
    },
  ]
  const overSightZones = await mongoCRUD.queryCollection(
    Zone,
    oversightPipeline,
  )
  thisVolunteer.overSightZones = overSightZones ? overSightZones : []

  return { data: thisVolunteer }
}

exports.getUsersWithSecurity = async function (
  securityModule,
  securityAccess,
  securityLevel,
  pagination,
) {
  const matches = {}

  if (securityModule) {
    matches['security.module'] = securityModule
  }

  if (securityAccess) {
    matches['security.access'] = securityAccess
  }

  if (securityLevel) {
    matches['security.level'] = securityLevel
  }

  let pipeline = [
    {
      $unwind: {
        path: '$security',
      },
    },
    { $match: matches },
    {
      $group: {
        _id: null,
        userID: {
          $addToSet: '$userID',
        },
      },
    },
    {
      $unwind: {
        path: '$userID',
      },
    },
    {
      $project: {
        _id: 0,
        userID: 1,
      },
    },
  ]

  pipeline = addPagination(pagination, pipeline)

  const userIdObjList = await mongoCRUD.queryCollection(Volunteer, pipeline)

  return { data: userIdObjList.map((x) => x.userID) }
}

exports.getUserIDsEmail = async function (user, userIDs) {
  return (
    await getVolunteers({
      user,
      criterion: {
        userID: userIDs,
      },
      columns: ['email', 'userID'],
    })
  ).data
}

exports.saveDocuments = async function (req) {
  const newDocument = new Document({
    documentID: uniqueIdentifier(),
    documentLink: req.file.key.split('/').pop(),
    documentType: req.body.documentType,
    documentAssociation: req.params.queryFilter1,
    associationName: req.params.queryFilter2,
    documentDescription: req.body.documentDescription,
    documentOwner: req.user.username,
    created: getUtcDateNow(),
    createdBy: req.user.userID,
    modified: getUtcDateNow(),
    modifiedBy: req.user.userID,
  })

  const {
    _doc: { _id, __v, ...result },
  } = await newDocument.save()
  return { data: result }
}

exports.markDocumentForDeletion = async function (req) {
  const filename = req.params.fileName
  const query = { documentID: req.params.documentID }
  const utcNow = getUtcDateNow()
  const update = {
    deleted: true,
    deletedDate: utcNow,
    deletedBy: req.user.userID,
    modified: utcNow,
    modifiedBy: req.user.userID,
  }

  await s3Connection.deleteFromS3(req.user.region, filename)

  return await Document.updateOne(query, update)
}

exports.deletePicture = async function (req) {
  await erasePicture(req)
  await s3Connection.deletePicsFromS3(req.user.region, req.params.fileName)
  return { data: [] }
}

exports.getPicture = async function (req) {
  return await s3Connection.getPhoto(req.user.region, req.params.photoLink)
}

exports.getVolunteerDocumentList = async function (req, matchQuery) {
  const defaultSort = { $sort: { documentType: req.sort.direction } }
  const sortQuery = req.sort.shouldUseQuerySort
    ? req.sort.pipelineSort
    : defaultSort
  let pipeline = [
    {
      $lookup: {
        from: 'documents',
        localField: 'email',
        foreignField: 'associationName',
        as: 'documents',
      },
    },
    {
      $unwind: {
        path: '$documents',
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: 'users',
        localField: 'documents.documentOwner',
        foreignField: 'email',
        as: 'uploadedBy',
      },
    },
    {
      $project: {
        _id: 0,
        userID: 1,
        status: 1,
        region: 1,
        documentID: '$documents.documentID',
        documentLink: '$documents.documentLink',
        documentType: '$documents.documentType',
        documentAssociation: '$documents.documentAssociation',
        associationName: '$documents.associationName',
        documentDescription: '$documents.documentDescription',
        documentOwner: '$documents.documentOwner',
        deleted: '$documents.deleted',
        created: '$documents.created',
        uploadedBy: { $arrayElemAt: ['$uploadedBy', 0] },
        uploadedOn: {
          $cond: {
            if: { $eq: ['$documents.created', undefined] },
            then: '$$NOW',
            else: '$documents.created',
          },
        },
      },
    },
    matchQuery,
    {
      $project: {
        'documents._id': 0,
        'documents.__v': 0,
      },
    },
    {
      $project: {
        userID: 1,
        documentID: 1,
        documentLink: 1,
        documentType: 1,
        documentAssociation: 1,
        associationName: 1,
        documentDescription: 1,
        documentOwner: 1,
        deleted: 1,
        status: 1,
        'uploadedBy.userID': '$uploadedBy.userID',
        'uploadedBy.firstName': '$uploadedBy.firstName',
        'uploadedBy.lastName': '$uploadedBy.lastName',
        'uploadedBy.email': '$uploadedBy.email',
        'uploadedBy.photo': '$uploadedBy.photoLink',
        created: 1,
        uploadedOn: 1,
      },
    },
    sortQuery,
  ]

  if (req.query.searchTerm) {
    pipeline.push({
      $match: {
        $or: [
          {
            'uploadedBy.firstName': {
              $regex: req.query.searchTerm,
              $options: 'i',
            },
          },
          {
            'uploadedBy.lastName': {
              $regex: req.query.searchTerm,
              $options: 'i',
            },
          },
          {
            'uploadedBy.email': { $regex: req.query.searchTerm, $options: 'i' },
          },
          { documentID: { $regex: req.query.searchTerm, $options: 'i' } },
          { documentLink: { $regex: req.query.searchTerm, $options: 'i' } },
          { documentType: { $regex: req.query.searchTerm, $options: 'i' } },
          { associationName: { $regex: req.query.searchTerm, $options: 'i' } },
          {
            documentDescription: {
              $regex: req.query.searchTerm,
              $options: 'i',
            },
          },
          { documentOwner: { $regex: req.query.searchTerm, $options: 'i' } },
        ],
      },
    })
  }

  const count = await mongoCRUD.countQuery(Volunteer, pipeline)

  pipeline = addPagination(req.pagination, pipeline)
  const info = await mongoCRUD.queryCollection(Volunteer, pipeline)

  return { ...count[0], data: info }
}

exports.getDocuments = async function (req) {
  try {
    const result = await s3Connection.getDocument(
      req.user.region,
      req.params.fileName,
    )
    return { data: result }
  } catch (err) {
    return { error: err.message }
  }
}

async function erasePicture(req) {
  let userID = req.params.userID

  const {
    _doc: { _id, __v, ...result },
  } = await Volunteer.findOneAndUpdate(
    { userID: userID },
    {
      photoLink: '',
      modified: getUtcDateNow(),
      modifiedBy: req.user.userID,
    },
  )

  return { data: result }
}

exports.savePicture = async function (req) {
  let userID = req.params.userID
  let key = req.file.key.split('/').pop()

  const {
    _doc: { _id, __v, ...result },
  } = await Volunteer.findOneAndUpdate(
    { userID: userID },
    {
      photoLink: key,
      modified: getUtcDateNow(),
      modifiedBy: req.user.userID,
    },
  )

  return { data: result }
}

exports.getFacilityVolForMeetings = async function (req) {
  let facilityID = req.params.facilityID
  const pipeline = [
    {
      $unwind: {
        path: '$facilities',
      },
    },
    {
      $match: {
        'facilities.facilityID': facilityID,
        'facilities.assignments.meetings': true,
      },
    },
    {
      $project: {
        _id: 0,
        name: {
          $concat: ['$lastName', ', ', '$firstName'],
        },
        id: '$userID',
        type: 'Volunteer',
      },
    },
    {
      $sort: {
        name: 1,
      },
    },
  ]
  return await mongoCRUD.queryCollection(Volunteer, pipeline)
}

exports.getAllVolunteersAccess = async function (req, matchQuery) {
  let pipeline = [
    matchQuery,
    {
      $project: {
        _id: 0,
        userID: 1,
      },
    },
  ]
  const volunteers = await mongoCRUD.queryCollection(Volunteer, pipeline)
  return volunteers.map((x) => x.userID)
}

exports.getVolunteerInfo = async function (user) {
  const Territories = require('../territories/territories.controller')
  user.territories = await Territories.getUserTerritories(user)
  return user
}

exports.getAllPaginatedVolunteers = async function (req, volunteerMatchQuery) {
  let defaultSort = [{ $sort: { fullName: req.sort.direction } }]
  let countOnly = false
  if (req.count && req.count === true) {
    countOnly = true
  }
  let exportOnly = false
  if (req.export && req.export === true) {
    exportOnly = true
  }
  let sortQuery = [{ $sort: { fullName: 1 } }]
  if (req.sort.shouldUseQuerySort) {
    sortQuery = req.sort.pipelineLowerSort
      ? req.sort.pipelineLowerSort
      : req.sort.shouldUseQuerySort
      ? req.sort.pipelineSort
      : defaultSort
  }

  const searchTerm = sanitize(req.body.advancedSearch?.searchTerm)
  const searchTerritoryName = sanitize(
    req.body.advancedSearch?.searchTerritoryName,
  )
  const searchTerritoryAbbreviation = sanitize(
    req.body.advancedSearch?.searchTerritoryAbbreviation,
  )
  // TODO: For the future, once territory has a broader reach to congregations
  // eslint-disable-next-line no-unused-vars
  const searchTerritoryID = sanitize(req.body.advancedSearch?.searchTerritoryID)
  const searchZoneID = sanitize(req.body.advancedSearch?.searchZoneID)

  if (
    !countOnly &&
    !searchTerritoryName &&
    !searchTerritoryAbbreviation &&
    !searchZoneID
  ) {
    return {
      err: 'Search must at least include either a searchZoneID or a searchTerritoryName and searchTerritoryAbbreviation, or all options',
    }
  }

  const hasTerritoryFilter =
    !isEmptyObject(searchTerritoryName || {}) &&
    !isEmptyObject(searchTerritoryAbbreviation || {})

  // const hasZoneFilter = !!searchZoneID  // this doesn't work..
  const hasZoneFilter = !isEmptyObject(searchZoneID || {})

  let initialMatch
  let initialMatch2
  if (hasTerritoryFilter && hasZoneFilter) {
    initialMatch = {
      $match: {
        $and: [
          {
            'zones.zoneID': { $in: searchZoneID },
          },
          {
            $or: [
              {
                'congregationAddress.state': { $in: searchTerritoryName },
              },
              {
                'congregationAddress.state': {
                  $in: searchTerritoryAbbreviation,
                },
              },
            ],
          },
        ],
      },
    }
    initialMatch2 = {
      $match: {
        $and: [
          {
            'congregations.zones.zoneID': { $in: searchZoneID },
          },
          {
            $or: [
              {
                'congregations.congregationAddress.state': {
                  $in: searchTerritoryName,
                },
              },
              {
                'congregations.congregationAddress.state': {
                  $in: searchTerritoryAbbreviation,
                },
              },
            ],
          },
        ],
      },
    }
  } else if (hasTerritoryFilter) {
    initialMatch = {
      $match: {
        $or: [
          {
            'congregationAddress.state': { $in: searchTerritoryName },
          },
          {
            'congregationAddress.state': { $in: searchTerritoryAbbreviation },
          },
        ],
      },
    }
    initialMatch2 = {
      $match: {
        $or: [
          {
            'congregations.congregationAddress.state': {
              $in: searchTerritoryName,
            },
          },
          {
            'congregations.congregationAddress.state': {
              $in: searchTerritoryAbbreviation,
            },
          },
        ],
      },
    }
  } else if (hasZoneFilter) {
    initialMatch = {
      $match: {
        'zones.zoneID': { $in: searchZoneID },
      },
    }
    initialMatch2 = {
      $match: {
        'congregations.zones.zoneID': { $in: searchZoneID },
      },
    }
  } else {
    initialMatch = {
      $match: {
        region: req.user.region,
      },
    }
    // eslint-disable-next-line no-unused-vars
    initialMatch2 = {
      $match: {
        'corresVolunteers.region': req.user.region,
      },
    }
  }

  let pipeline = [
    initialMatch,
    {
      $lookup: {
        from: 'users',
        localField: 'congregationID',
        foreignField: 'congregationID',
        as: 'volunteers',
      },
    },
    {
      $unwind: {
        path: '$volunteers',
      },
    },
    {
      $addFields: {
        'volunteers.zones.zoneID': '$zones.zoneID',
        'volunteers.congregationAddress': '$congregationAddress',
        'volunteers.fullName': {
          $concat: ['$volunteers.lastName', ', ', '$volunteers.firstName'],
        },
        'volunteers.congregationName': '$congregationName',
        'volunteers.primaryAddress': {
          $first: '$congregationAddress',
        },
        'volunteers.congregationCity': {
          $first: '$congregationAddress.city',
        },
        'volunteers.congregationState': {
          $first: '$congregationAddress.state',
        },
        'volunteers.congregationLocation': {
          $concat: [
            {
              $first: '$congregationAddress.city',
            },
            ', ',
            {
              $first: '$congregationAddress.state',
            },
          ],
        },
        'volunteers.gender': '$volunteers.volunteerGender',
        'volunteers.theocraticPrivileges':
          '$volunteers.appointment.responsibility',
        'volunteers.volunteerApprovedFor': '$volunteers.facilities.assignments',
        'volunteers.volunteerLanguageProficiency': '$volunteers.language',
        'volunteers.volunteerLanguages': '$volunteers.langs.languageName',
        'volunteers.servingAsSize': {
          $size: {
            $cond: [
              { $isArray: '$volunteers.appointment.responsibility' },
              '$volunteers.appointment.responsibility',
              [],
            ],
          },
        },
      },
    },
    {
      $replaceRoot: {
        newRoot: '$volunteers',
      },
    },
    ...sortQuery,
    {
      $lookup: {
        from: 'languages',
        localField: 'language.languageID',
        foreignField: 'languageID',
        as: 'languages',
      },
    },
  ]

  if (JSON.stringify(volunteerMatchQuery).search('inPersonCnt') >= 0) {
    pipeline.push(
      {
        $lookup: {
          from: 'inmates',
          localField: 'userID',
          foreignField: 'assignedInPerson.userID',
          //localField: 'email',
          //foreignField: 'volAssigned',
          as: 'assignedInmates',
        },
      },
      {
        $addFields: {
          inPersonCnt: {
            $size: '$assignedInmates',
          },
        },
      },
    )
  }

  if (JSON.stringify(volunteerMatchQuery).search('corresCnt') >= 0) {
    pipeline.push(
      {
        $lookup: {
          from: 'inmates',
          localField: 'userID',
          foreignField: 'assignedCorrespondence.userID',
          as: 'correspondenceInmates',
        },
      },
      {
        $addFields: {
          corresCnt: {
            $size: '$correspondenceInmates',
          },
        },
      },
    )
  }
  pipeline.push(
    {
      $addFields: {
        status: '$status',
        facilityContact: {
          $toBool: {
            $cond: {
              if: {
                $eq: [
                  {
                    $reduce: {
                      input: '$facilities',
                      initialValue: '',
                      in: {
                        $cond: {
                          if: {
                            $and: [
                              {
                                $eq: ['$$this.assignments.contact', true],
                              },
                            ],
                          },
                          then: 'true',
                          else: '$$value',
                        },
                      },
                    },
                  },
                  'true',
                ],
              },
              then: true,
              else: false,
            },
          },
        },
        volunteerApprovedForCorrespondence: {
          $toBool: {
            $cond: {
              if: {
                $eq: [
                  {
                    $reduce: {
                      input: '$facilities',
                      initialValue: '',
                      in: {
                        $cond: {
                          if: {
                            $and: [
                              {
                                $eq: [
                                  '$$this.assignments.correspondence',
                                  true,
                                ],
                              },
                            ],
                          },
                          then: 'true',
                          else: '$$value',
                        },
                      },
                    },
                  },
                  'true',
                ],
              },
              then: true,
              else: false,
            },
          },
        },
        volunteerApprovedForContact: {
          $toBool: {
            $cond: {
              if: {
                $eq: [
                  {
                    $reduce: {
                      input: '$facilities',
                      initialValue: '',
                      in: {
                        $cond: {
                          if: {
                            $and: [
                              {
                                $eq: ['$$this.assignments.contact', true],
                              },
                            ],
                          },
                          then: 'true',
                          else: '$$value',
                        },
                      },
                    },
                  },
                  'true',
                ],
              },
              then: true,
              else: false,
            },
          },
        },
        volunteerApprovedForInPersonVisits: {
          $toBool: {
            $cond: {
              if: {
                $eq: [
                  {
                    $reduce: {
                      input: '$facilities',
                      initialValue: '',
                      in: {
                        $cond: {
                          if: {
                            $and: [
                              {
                                $eq: [
                                  '$$this.assignments.inPersonVisits',
                                  true,
                                ],
                              },
                            ],
                          },
                          then: 'true',
                          else: '$$value',
                        },
                      },
                    },
                  },
                  'true',
                ],
              },
              then: true,
              else: false,
            },
          },
        },
        volunteerApprovedForIclw: {
          $toBool: {
            $cond: {
              if: {
                $eq: [
                  {
                    $reduce: {
                      input: '$facilities',
                      initialValue: '',
                      in: {
                        $cond: {
                          if: {
                            $and: [
                              {
                                $eq: ['$$this.assignments.iclw', true],
                              },
                            ],
                          },
                          then: 'true',
                          else: '$$value',
                        },
                      },
                    },
                  },
                  'true',
                ],
              },
              then: true,
              else: false,
            },
          },
        },
        volunteerApprovedForMeetings: {
          $toBool: {
            $cond: {
              if: {
                $eq: [
                  {
                    $reduce: {
                      input: '$facilities',
                      initialValue: '',
                      in: {
                        $cond: {
                          if: {
                            $and: [
                              {
                                $eq: ['$$this.assignments.meetings', true],
                              },
                            ],
                          },
                          then: 'true',
                          else: '$$value',
                        },
                      },
                    },
                  },
                  'true',
                ],
              },
              then: true,
              else: false,
            },
          },
        },
        isBranchRep: {
          $ifNull: ['$isBranchRep', false],
        },
        deleted: {
          $ifNull: ['$deleted', false],
        },
        zoneContact: {
          $cond: {
            if: { $eq: ['$role', 'zone'] },
            then: 'true',
            else: 'false',
          },
        },
        regionalContact: {
          $cond: {
            if: { $eq: ['$role', 'regional'] },
            then: 'true',
            else: 'false',
          },
        },
        branchContact: '$isBranchRep',
      },
    },
    volunteerMatchQuery,
    {
      $lookup: {
        from: 'languages',
        localField: 'languages.languageID',
        foreignField: 'languageID',
        as: 'langs',
      },
    },
    {
      $project: {
        _id: 0,
        regionID: '$region',
        zoneID: '$zones.zoneID',
        userID: 1,
        lastName: 1,
        firstName: 1,
        fullName: 1,
        facilities: '$facilities.facilityID',
        congregationID: 1,
        congregationName: 1,
        congregationCity: 1,
        congregationState: 1,
        congregationLocation: 1,
        phone: 1,
        homePhone: 1,
        email: 1,
        state: 1,
        region: 1,
        volunteerLanguageProficiency: {
          $map: {
            input: '$volunteerLanguageProficiency',
            as: 'volunteerLanguageProficiency',
            in: {
              $mergeObjects: [
                '$$volunteerLanguageProficiency',
                {
                  $arrayElemAt: [
                    {
                      $filter: {
                        input: '$langs',
                        as: 'lang',
                        cond: {
                          $eq: [
                            '$$volunteerLanguageProficiency.languageID',
                            '$$lang.languageID',
                          ],
                        },
                      },
                    },
                    0,
                  ],
                },
              ],
            },
          },
        },
        status: 1,
        photoLink: 1,
        role: 1,
        gender: 1,
        deleted: 1,
        theocraticPrivileges: 1,
        volunteerApprovedFor: 1,
        isICLWContact: 1,
        isICLWVolunteer: 1,
        lastLogin: '$lastAccess',
        inPersonCnt: { $ifNull: [`$inPersonCnt`, 0] },
        corresCnt: { $ifNull: [`$corresCnt`, 0] },
        facilityContact: 1,
        zoneContact: 1,
        regionalContact: 1,
        branchContact: 1,
        isBranchRep: 1,
        servingAsSize: 1,
      },
    },
    {
      $project: {
        _id: 0,
        lowerName: 0,
      },
    },
  )

  if (searchTerm) {
    pipeline.push({
      $match: {
        fullName: { $regex: searchTerm, $options: 'i' },
      },
    })
  }

  if (exportOnly) {
    {
      pipeline.push(
        {
          $lookup: {
            from: 'zones',
            localField: 'zoneID',
            foreignField: 'zoneID',
            as: 'zones',
          },
        },
        {
          $lookup: {
            from: 'facilities',
            localField: 'facilities',
            foreignField: 'facilityID',
            as: 'facilities',
          },
        },
        {
          $addFields: {
            Zone: {
              $reduce: {
                input: '$zones.zoneName',
                initialValue: '',
                in: {
                  $concat: ['$$value', '$$this', ' '],
                },
              },
            },
            Facilities: {
              $reduce: {
                input: '$facilities.locationName',
                initialValue: '',
                in: {
                  $concat: ['$$value', '$$this', ', '],
                },
              },
            },
            Languages: {
              $reduce: {
                input: '$volunteerLanguageProficiency',
                initialValue: '',
                in: {
                  $concat: ['$$value', '$$this.languageName', ', '],
                },
              },
            },
            Responsibilities: {
              $reduce: {
                input: '$theocraticPrivileges',
                initialValue: '',
                in: {
                  $concat: ['$$value', '$$this', ', '],
                },
              },
            },
          },
        },
        {
          $project: {
            LastName: '$lastName',
            FirstName: '$firstName',
            Email: '$email',
            Mobile: '$phone',
            Congregation: {
              $concat: ['$congregationName', ' - ', '$congregationLocation'],
            },
            Zone: 1,
            Facilities: 1,
            Responsibilities: 1,
            Languages: 1,
            Role: '$role',
            Status: '$status',
            LastLogin: '$lastLogin',
          },
        },
      )
    }
  }
  const count = mongoCRUD.countQuery(Congregation, pipeline)

  if (countOnly) {
    const [c] = await Promise.all([count])
    return c[0] ? c[0] : { total: 0 }
  }

  pipeline = addPagination(req.pagination, pipeline)

  const volunteers = mongoCRUD.queryCollection(Congregation, pipeline)
  const [c, v] = await Promise.allSettled([count, volunteers])
  if (!exportOnly) {
    // eslint-disable-next-line no-unused-vars
    let result = await s3Connection.determinePresignedURLs(
      req,
      v?.value,
      'Photo',
      'photoLink',
      'photoLinkURL',
    )
  }

  return { total: c.value[0]?.total, data: v?.value }
}

exports.getVolunteerByID = async function (req, matchQuery) {
  // eslint-disable-next-line no-unused-vars
  let searchQuery = {}
  // eslint-disable-next-line no-unused-vars
  let sortQuery = {}
  let pipeline = [
    matchQuery,
    {
      $lookup: {
        from: 'congregations',
        localField: 'congregationID',
        foreignField: 'congregationID',
        as: 'congregation',
      },
    },
    {
      $unwind: {
        path: '$congregation',
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $unwind: {
        path: '$congregation.congregationAddress',
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: 'circuits',
        localField: 'congregation.circuitID',
        foreignField: 'circuitID',
        as: 'circuit',
      },
    },
    {
      $lookup: {
        from: 'zones',
        localField: 'congregation.zones.zoneID',
        foreignField: 'zoneID',
        as: 'zone',
      },
    },
    {
      $lookup: {
        from: 'textservices',
        localField: 'serviceProviderID',
        foreignField: 'serviceID',
        as: 'serviceProvider',
      },
    },
    {
      $unwind: {
        path: '$serviceProvider',
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $project: {
        _id: 0,
        userID: 1,
        firstName: 1,
        lastName: 1,
        dateofbirth: '$birthDate',
        gender: '$volunteerGender',
        maritalStatus: 1,
        tbTesting: '$tbTestingDate',
        email: 1,
        homephone: '$homePhone',
        mobilephone: '$phone',
        textOptIn: 1,
        communicationInfo: 1,
        serviceProviderID: '$serviceProvider.serviceID',
        provider: '$serviceProvider.provider',
        serviceProvidercommunicationInfo: '$serviceProvider.communicationInfo',
        address: 1,
        city: 1,
        state: 1,
        zip: '$zipCode',
        emergencyContactName: '$emergencyContact',
        emergencyContactPhone: '$emergencyContactNumber',
        emergencyContactRelationship: '$emergencyContactRelationship',
        languageProficiency: '$language',
        circuitID: {
          $arrayElemAt: ['$circuit.circuitID', 0],
        },
        circuitName: {
          $arrayElemAt: ['$circuit.circuitName', 0],
        },
        zoneID: {
          $arrayElemAt: ['$zone.zoneID', 0],
        },
        zoneName: {
          $arrayElemAt: ['$zone.zoneName', 0],
        },
        congregationID: '$congregation.congregationID',
        congregation: {
          $concat: [
            '$congregation.congregationName',
            ' - ',
            '$congregation.congregationAddress.city',
            ', ',
            '$congregation.congregationAddress.state',
          ],
        },
        congregationAddress: '$congregation.congregationAddress',
        photo: '$photoLink',
        dateofBaptism: '$baptismDate',
        region: 1,
        status: 1,
        servingAs: '$appointment',
        isICLWContact: '$isICLWContact',
        isICLWVolunteer: '$isICLWVolunteer',
      },
    },
  ]
  let volunteer = await mongoCRUD.queryCollection(Volunteer, pipeline)
  // eslint-disable-next-line no-unused-vars
  let result2 = await s3Connection.determinePresignedURLs(
    req,
    volunteer,
    'Photo',
    'photo',
    'photoURL',
  )
  if (volunteer[0]) {
    return { data: volunteer[0] }
  }
  return null
}

const Documents = require('../document/documents.model')
const Experiences = require('../experience/experiences.model')
const MeetingTemplates = require('../meetingtemplates/meetingtemplates.model')
const Studies = require('../study/studies.model')

async function performEmailConversion(req, userID, fromEmail, toEmail) {
  if (!userID || !fromEmail || !toEmail) {
    return []
  }
  const inUserID = req.user.userID
  const documents = await convertDocumentsEmail(inUserID, fromEmail, toEmail)
  const experiences = await convertExperiencesEmail(
    inUserID,
    fromEmail,
    toEmail,
  )
  const meetingtemplates = await convertMeetingTemplatesEmail(
    inUserID,
    fromEmail,
    toEmail,
  )
  const studies = await convertStudiesEmail(inUserID, fromEmail, toEmail)
  const volunteers = await convertVolunteersEmail(inUserID, fromEmail, toEmail)
  const facilities = await convertFacilitiesEmail(inUserID, fromEmail, toEmail)
  return {
    volunteers: volunteers,
    documents: documents,
    experiences: experiences,
    meetingtemplates: meetingtemplates,
    studies: studies,
    facilities: facilities,
  }
}
exports.performEmailConversion = performEmailConversion

async function convertDocumentsEmail(inUserID, fromEmail, toEmail) {
  let pipeline = [
    {
      $match: {
        $or: [{ associationName: fromEmail }, { documentOwner: fromEmail }],
      },
    },
    {
      $project: {
        documentID: 1,
        associationName: 1,
        documentOwner: 1,
      },
    },
  ]
  let documentsEmail = await mongoCRUD.queryCollection(Documents, pipeline)

  let query = {}
  let utcNow = getUtcDateNow()
  let update = {}

  let promises = []
  documentsEmail.forEach((d) => {
    const { documentID } = d
    if (!documentID) return documentsEmail
    query = { documentID: documentID }
    update = {
      associationName: toEmail,
      documentOwner: toEmail,
      modified: utcNow,
      modifiedBy: inUserID,
    }
    promises.push(Documents.updateOne(query, update))
  })
  await Promise.allSettled(promises)

  return documentsEmail
}

async function convertExperiencesEmail(inUserID, fromEmail, toEmail) {
  let pipeline = [
    {
      $match: {
        $or: [{ author: fromEmail }],
      },
    },
    {
      $project: {
        experienceID: 1,
        author: 1,
      },
    },
  ]
  let experiencesEmail = await mongoCRUD.queryCollection(Experiences, pipeline)

  let query = {}
  let utcNow = getUtcDateNow()
  let update = {}

  let promises = []
  experiencesEmail.forEach((e) => {
    const { experienceID } = e
    if (!experienceID) return experiencesEmail
    query = { experienceID: experienceID }
    update = {
      author: toEmail,
      modified: utcNow,
      modifiedBy: inUserID,
    }
    promises.push(Experiences.updateOne(query, update))
  })
  await Promise.allSettled(promises)

  return experiencesEmail
}

async function convertMeetingTemplatesEmail(inUserID, fromEmail, toEmail) {
  let pipeline = [
    {
      $match: {
        $or: [{ creator: fromEmail }],
      },
    },
    {
      $project: {
        templateID: 1,
        creator: 1,
      },
    },
  ]
  let meetingtemplatesEmail = await mongoCRUD.queryCollection(
    MeetingTemplates,
    pipeline,
  )

  let query = {}
  let utcNow = getUtcDateNow()
  let update = {}

  let promises = []
  meetingtemplatesEmail.forEach((t) => {
    const { templateID } = t
    if (!templateID) return meetingtemplatesEmail
    query = { templateID: templateID }
    update = {
      creator: toEmail,
      modified: utcNow,
      modifiedBy: inUserID,
    }
    promises.push(MeetingTemplates.updateOne(query, update))
  })
  await Promise.allSettled(promises)

  return meetingtemplatesEmail
}

async function convertStudiesEmail(inUserID, fromEmail, toEmail) {
  let pipeline = [
    {
      $match: {
        $or: [{ volunteer: fromEmail }],
      },
    },
    {
      $project: {
        studyID: 1,
        volunteer: 1,
      },
    },
  ]
  let studiesEmail = await mongoCRUD.queryCollection(Studies, pipeline)

  let query = {}
  let utcNow = getUtcDateNow()
  let update = {}

  let promises = []
  studiesEmail.forEach((s) => {
    const { studyID } = s
    if (!studyID) return studiesEmail
    query = { studyID: studyID }
    update = {
      volunteer: toEmail,
      modified: utcNow,
      modifiedBy: inUserID,
    }
    promises.push(Studies.updateOne(query, update))
  })
  await Promise.allSettled(promises)

  return studiesEmail
}

async function convertVolunteersEmail(inUserID, fromEmail, toEmail) {
  let pipeline = [
    {
      $match: {
        $or: [{ username: fromEmail }],
      },
    },
    {
      $project: {
        userID: 1,
        username: 1,
      },
    },
  ]
  let volunteersEmail = await mongoCRUD.queryCollection(Volunteer, pipeline)

  let query = {}
  let utcNow = getUtcDateNow()
  let update = {}

  let promises = []
  volunteersEmail.forEach((v) => {
    const { userID } = v
    if (!userID) return volunteersEmail
    query = { userID: userID }
    update = {
      email: toEmail,
      username: toEmail,
      modified: utcNow,
      modifiedBy: inUserID,
    }
    promises.push(Volunteer.updateOne(query, update))
  })
  await Promise.allSettled(promises)

  return volunteersEmail
}

async function convertFacilitiesEmail(inUserID, fromEmail, toEmail) {
  let pipeline = [
    {
      $match: {
        $or: [{ overseer: fromEmail }],
      },
    },
    {
      $project: {
        facilityID: 1,
        overseer: 1,
      },
    },
  ]
  let facilitiesEmail = await mongoCRUD.queryCollection(Facilities, pipeline)

  let query = {}
  let utcNow = getUtcDateNow()
  let update = {}

  let promises = []
  facilitiesEmail.forEach((f) => {
    const { facilityID } = f
    if (!facilityID) return facilitiesEmail
    query = { facilityID: facilityID }
    update = {
      overseer: toEmail,
      modified: utcNow,
      modifiedBy: inUserID,
    }
    promises.push(Facilities.updateOne(query, update))
  })
  await Promise.allSettled(promises)

  return facilitiesEmail
}

async function getUserEmailByUserID(userID) {
  let pipeline = [
    {
      $match: {
        $and: [{ userID: { $eq: userID } }, { deleted: { $ne: true } }],
      },
    },
    {
      $project: {
        userID: 1,
        email: 1,
      },
    },
  ]
  let volunteerEmail = await mongoCRUD.queryCollection(Volunteer, pipeline)
  return volunteerEmail
}
exports.getUserEmailByUserID = getUserEmailByUserID

exports.updateVolunteer = async function (req) {
  let query = { userID: req.params.id }
  const existingVolunteer = req.body
  let updatedVolunteer = existingVolunteer
  if (!updatedVolunteer.region) {
    updatedVolunteer.region = req.user.region
  }

  const existingEmail = await getUserEmailByUserID(req.params.id)
  const emailChanged =
    existingEmail[0].email && existingEmail[0].email !== existingVolunteer.email
      ? true
      : false

  updatedVolunteer.firstName = orNull(existingVolunteer.firstName)
  updatedVolunteer.lastName = orNull(existingVolunteer.lastName)
  updatedVolunteer.birthDate = orNull(existingVolunteer.dateofbirth)
  updatedVolunteer.volunteerGender = orNull(existingVolunteer.gender)
  updatedVolunteer.maritalStatus = orNull(existingVolunteer.maritalStatus)
  updatedVolunteer.tbTestingDate = orNull(existingVolunteer.tbTesting)
  updatedVolunteer.email = orNull(existingVolunteer.email.toLowerCase())
  updatedVolunteer.phone = orNull(existingVolunteer.mobilephone)
  updatedVolunteer.homePhone = orNull(existingVolunteer.homephone)
  updatedVolunteer.textOptIn = existingVolunteer.textOptIn
  updatedVolunteer.communicationInfo = orNull(
    existingVolunteer.communicationInfo,
  )
  updatedVolunteer.serviceProviderID = orNull(
    existingVolunteer.serviceProviderID,
  )
  updatedVolunteer.address = orNull(existingVolunteer.address)
  updatedVolunteer.city = orNull(existingVolunteer.city)
  updatedVolunteer.state = orNull(existingVolunteer.state)
  updatedVolunteer.zipCode = orNull(existingVolunteer.zip)
  updatedVolunteer.emergencyContact = orNull(
    existingVolunteer.emergencyContactName,
  )
  updatedVolunteer.emergencyContactRelationship = orNull(
    existingVolunteer.emergencyContactRelationship,
  )
  updatedVolunteer.emergencyContactNumber = orNull(
    existingVolunteer.emergencyContactPhone,
  )
  updatedVolunteer.language = existingVolunteer.languageProficiency
  if (existingVolunteer.congregationID) {
    updatedVolunteer.congregationID = existingVolunteer.congregationID
  }
  if (existingVolunteer.role) {
    updatedVolunteer.role = existingVolunteer.role
    updatedVolunteer.security = await createSecurityArray(
      existingVolunteer.role,
    )
  }
  updatedVolunteer.region = orNull(existingVolunteer.region)
  updatedVolunteer.status = orNull(existingVolunteer.status)
  updatedVolunteer.baptismDate = orNull(existingVolunteer.dateofBaptism)
  updatedVolunteer.appointment = orNull(existingVolunteer.servingAs)
  updatedVolunteer.isICLWContact = existingVolunteer.isICLWContact
  updatedVolunteer.isICLWVolunteer = existingVolunteer.isICLWVolunteer
  updatedVolunteer.username = orNull(existingVolunteer.email.toLowerCase())

  updatedVolunteer.modified = getUtcDateNow()
  updatedVolunteer.modifiedBy = req.user.email

  if (updatedVolunteer.status === 'Approved' || emailChanged) {
    const validationErrors = await VolunteerValidation.validateExistingEmail(
      updatedVolunteer,
      'update',
    )

    if (validationErrors.length > 0) {
      return { errors: validationErrors }
    }
  }

  delete updatedVolunteer['_id']
  delete updatedVolunteer['__v']
  delete updatedVolunteer['created']
  delete updatedVolunteer['createdBy']
  delete updatedVolunteer['applicantID']
  delete updatedVolunteer['userID']

  const {
    _doc: {
      _id,
      __v,
      // eslint-disable-next-line no-unused-vars
      ...result
    },
  } = await mongoCRUD.updateCollection(Volunteer, query, updatedVolunteer)

  if (emailChanged) {
    await performEmailConversion(
      req,
      existingEmail[0].userID,
      existingEmail[0].email,
      existingVolunteer.email.toLowerCase(),
    )
  }

  return { data: ['success'] }
}

exports.updateVolunteerUILanguage = async function (req) {
  let query = { userID: req.user.userID }

  const existingVolunteer = req.body

  const utcNow = getUtcDateNow()
  const update = {
    uiLanguage: existingVolunteer.uiLanguage,
    modified: utcNow,
    modifiedBy: req.user.userID,
  }
  await Volunteer.updateOne(query, update)

  return { data: ['success'] }
}

exports.enableDisableVolunteer = async function (req) {
  const query = { userID: req.params.userID }
  const utcNow = getUtcDateNow()
  const update = {
    status: req.params.status,
    modified: utcNow,
    modifiedBy: req.user.userID,
  }
  await Volunteer.updateOne(query, update)
  return { data: [] }
}

exports.markVolunteerForDeletion = async function (req) {
  const query = { userID: req.params.id }
  const utcNow = getUtcDateNow()
  const update = {
    status: 'inactive',
    facilities: [],
    deleted: true,
    deletedDate: utcNow,
    deletedBy: req.user.userID,
    modified: utcNow,
    modifiedBy: req.user.userID,
  }
  const shouldProcessDeletion = true
  if (shouldProcessDeletion) {
    await removeVolunteerAccess(req.user, req.params.id)
    return await Volunteer.updateOne(query, update)
  } else {
    return { message: 'Volunteer still has other data associated with it' }
  }
}

async function removeVolunteerAccess(user, userID) {
  let pipeline = [
    {
      $match: {
        userID: userID,
      },
    },
    {
      $project: {
        userID: 1,
        facilityID: '$facilities.facilityID',
        username: 1,
        email: 1,
        facilities: 1,
      },
    },
  ]
  let promises = []
  const volunteer = await mongoCRUD.queryCollection(Volunteer, pipeline)
  const volunteerWork = volunteer[0]
  const utcNow = getUtcDateNow()

  for (let i = 0; i < volunteerWork.facilities.length; i++) {
    const facility = volunteerWork.facilities[i]
    if (facility.assignments.contact) {
      pipeline = [
        {
          $match: {
            $or: [
              { assistantContacts: volunteerWork.userID },
              { overseer: volunteerWork.email },
            ],
          },
        },
        {
          $project: {
            facilityID: 1,
            assistantContacts: 1,
            overseer: 1,
          },
        },
      ]
      const facilityContacts = await mongoCRUD.queryCollection(
        Facilities,
        pipeline,
      )
      facilityContacts.forEach((fc) => {
        // eslint-disable-next-line no-unused-vars
        const { facilityID, assistantContacts, overseer } = fc
        const newFCs = assistantContacts.filter(
          (fc2) => fc2 !== volunteerWork.userID,
        )
        const newOverseer =
          fc.overseer === volunteerWork.email ? null : fc.overseer
        let query = { facilityID: facilityID }
        let update = {
          assistantContacts: newFCs,
          overseer: newOverseer,
          modified: utcNow,
          modifiedBy: user.userID,
        }
        promises.push(Facilities.updateOne(query, update))
      })
    }
    if (facility.assignments.correspondence) {
      pipeline = [
        {
          $match: {
            'assignedCorrespondence.userID': {
              $eq: volunteerWork.userID,
            },
          },
        },
        {
          $project: {
            inmateID: 1,
            assignedCorrespondence: 1,
          },
        },
      ]
      const inmateCorrespondence = await mongoCRUD.queryCollection(
        Inmates,
        pipeline,
      )
      inmateCorrespondence.forEach((ic) => {
        const { inmateID, assignedCorrespondence } = ic
        const newACs = assignedCorrespondence.filter(
          (ic2) => ic2.userID !== volunteerWork.userID,
        )
        let query = { inmateID: inmateID }
        let update = {
          assignedCorrespondence: newACs,
          modified: utcNow,
          modifiedBy: user.userID,
        }
        promises.push(Inmates.updateOne(query, update))
      })
    }
    if (facility.assignments.inPersonVisits) {
      pipeline = [
        {
          $match: {
            'assignedInPerson.userID': {
              $eq: volunteerWork.userID,
            },
          },
        },
        {
          $project: {
            inmateID: 1,
            assignedInPerson: 1,
          },
        },
      ]
      const inmateInperson = await mongoCRUD.queryCollection(Inmates, pipeline)
      inmateInperson.forEach((ip) => {
        const { inmateID, assignedInPerson } = ip
        const newAPs = assignedInPerson.filter(
          (ip2) => ip2.userID !== volunteerWork.userID,
        )
        let query = { inmateID: inmateID }
        let update = {
          assignedInPerson: newAPs,
          modified: utcNow,
          modifiedBy: user.userID,
        }
        promises.push(Inmates.updateOne(query, update))
      })
    }
  }
  pipeline = [
    {
      $match: {
        volunteer: volunteerWork.email,
      },
    },
    {
      $project: {
        studyID: 1,
        volunteer: 1,
      },
    },
  ]
  const deleteStudies = await mongoCRUD.queryCollection(Study, pipeline)
  deleteStudies.forEach((ds) => {
    const { studyID } = ds
    promises.push(
      mongoCRUD.deleteCollectionDocument(Study, { studyID: studyID }),
    )
  })
  Promise.allSettled(promises)
  await Meetings.declineMeetingParts(userID)

  return volunteer
}

exports.getVolunteerApplicantByID = async function (req, matchQuery) {
  let pipeline = [
    {
      $project: {
        _id: 0,
      },
    },
    {
      $lookup: {
        from: 'applicants',
        localField: 'userID',
        foreignField: 'userID',
        as: 'applicant',
      },
    },
    {
      $unwind: {
        path: '$applicant',
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: 'users',
        localField: 'approvedByUserID',
        foreignField: 'userID',
        as: 'approver',
      },
    },
    {
      $unwind: {
        path: '$approver',
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: 'steps',
        localField: 'applicant.stepTemplateID',
        foreignField: 'stepTemplateID',
        as: 'stepTemplate',
      },
    },
    {
      $unwind: {
        path: '$stepTemplate',
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: 'congregations',
        localField: 'approver.congregationID',
        foreignField: 'congregationID',
        as: 'approverCongregations',
      },
    },
    {
      $addFields: {
        'approver.congregation': {
          $arrayElemAt: ['$approverCongregations', 0],
        },
      },
    },
    {
      $addFields: {
        'approver.congregationAddress': {
          $arrayElemAt: ['$approver.congregation.congregationAddress', 0],
        },
        hasCriminalRecord: {
          $switch: {
            branches: [
              {
                case: {
                  $eq: [
                    true,
                    {
                      $regexMatch: {
                        input: '$criminalRecord',
                        regex: 'yes',
                        options: 'i',
                      },
                    },
                  ],
                },
                then: 'Yes',
              },
            ],
            default: 'No',
          },
        },
      },
    },
    {
      $project: {
        'applicant.steps._id': 0,
      },
    },
    matchQuery,
    {
      $project: {
        applicantID: 1,
        userID: 1,
        criminalRecord: '$hasCriminalRecord',
        mondayAvailability: '$applicant.mondayAvailability',
        tuesdayAvailability: '$applicant.tuesdayAvailability',
        wednesdayAvailability: '$applicant.wednesdayAvailability',
        thursdayAvailability: '$applicant.thursdayAvailability',
        fridayAvailability: '$applicant.fridayAvailability',
        saturdayAvailability: '$applicant.saturdayAvailability',
        sundayAvailability: '$applicant.sundayAvailability',
        approvedDate: '$approvedDate',
        stepTemplateID: '$applicant.stepTemplateID',
        templateName: '$stepTemplate.stepTemplateName',
        steps: '$applicant.steps',
        'approvedBy.userID': '$approver.userID',
        'approvedBy.userName': '$approver.userName',
        'approvedBy.firstName': '$approver.firstName',
        'approvedBy.lastName': '$approver.lastName',
        'approvedBy.photoLink': '$approver.photoLink',
        'approvedBy.congregationName':
          '$approver.congregation.congregationName',
        'approvedBy.congregationCity': '$approver.congregationAddress.city',
        'approvedBy.congregationState': '$approver.congregationAddress.state',
      },
    },
  ]

  let applicant = await mongoCRUD.queryCollection(Volunteer, pipeline)
  if (applicant[0]) {
    // Add signed url to applicant
    await s3Connection.determinePresignedURLs(
      req,
      applicant,
      'Photo',
      'photoLink',
      'photoLinkURL',
    )

    return { data: applicant[0] }
  }
  return null
}

exports.getCongregationsByTerritory = async function (req) {
  let defaultSort = [{ $sort: { congregationName: req.sort.direction } }]
  let sortQuery = [{ $sort: { congregationName: 1 } }]
  if (req.sort.shouldUseQuerySort) {
    sortQuery = req.sort.pipelineLowerSort
      ? req.sort.pipelineLowerSort
      : req.sort.shouldUseQuerySort
      ? req.sort.pipelineSort
      : defaultSort
  }

  const searchTerm = sanitize(req.query?.searchTerm)
  const searchTerritoryName = sanitize(req.query?.searchTerritoryName)
  const searchTerritoryAbbreviation = sanitize(
    req.query?.searchTerritoryAbbreviation,
  )
  // TODO: For the future, once territory has a broader reach to congregations
  // eslint-disable-next-line no-unused-vars
  const searchTerritoryID = sanitize(req.query?.searchTerritoryID)
  // eslint-disable-next-line no-unused-vars
  const hasTerritoryFilter =
    !!searchTerritoryName && !!searchTerritoryAbbreviation

  let pipeline = [
    {
      $match: {
        $or: [
          {
            'congregationAddress.state': searchTerritoryName,
          },
          {
            'congregationAddress.state': searchTerritoryAbbreviation,
          },
        ],
      },
    },
    {
      $addFields: {
        primaryAddress: {
          $first: '$congregationAddress',
        },
      },
    },
    {
      $project: {
        'primaryAddress._id': 0,
      },
    },
    {
      $project: {
        _id: 0,
        regionID: '$region',
        zoneID: '$zones.zoneID',
        languageID: 1,
        congregationID: '$congregationID',
        congregationName: '$congregationName',
        primaryAddress: 1,
        congregationCity: '$primaryAddress.city',
        congregationState: '$primaryAddress.state',
        congregationLocation: {
          $concat: ['$primaryAddress.city', ', ', '$primaryAddress.state'],
        },
      },
    },
    ...sortQuery,
  ]
  if (searchTerm) {
    pipeline.push({
      $match: {
        congregationName: { $regex: searchTerm, $options: 'i' },
        congregationLocation: { $regex: searchTerm, $options: 'i' },
      },
    })
  }
  console.log(JSON.stringify(pipeline))
  const count = await mongoCRUD.countQuery(Congregation, pipeline)

  const congregations = await mongoCRUD.queryCollection(Congregation, pipeline)

  return { ...count[0], data: congregations }
}

exports.getVolunteersByRegion = async function (regionID) {
  //async function getVolunteersByRegion(regionID) {
  const pipeline = [
    {
      $match: {
        regionID: regionID,
      },
    },
    {
      $lookup: {
        from: 'territories',
        localField: 'regionName',
        foreignField: 'territoryRegion',
        as: 'territories',
      },
    },
    {
      $unwind: {
        path: '$territories',
      },
    },
    {
      $unwind: {
        path: '$territories.zones',
        preserveNullAndEmptyArrays: false,
      },
    },
    {
      $lookup: {
        from: 'congregations',
        localField: 'territories.zones.zoneID',
        foreignField: 'zones.zoneID',
        as: 'congregations',
      },
    },
    {
      $unwind: {
        path: '$congregations',
      },
    },
    {
      $lookup: {
        from: 'users',
        localField: 'congregations.congregationID',
        foreignField: 'congregationID',
        as: 'volunteers',
      },
    },
    {
      $unwind: {
        path: '$volunteers',
      },
    },
    {
      $group: {
        _id: {
          regionID: '$regionID',
          regionName: '$regionName',
          territory: '$territories.name',
          territoryAbbreviation: '$territories.abbreviation',
        },
        volunteers: {
          $addToSet: {
            userID: '$volunteers.userID',
            firstName: '$volunteers.firstName',
            lastName: '$volunteers.lastName',
          },
        },
      },
    },
    {
      $project: {
        _id: 0,
        regionID: '$_id.regionID',
        regionName: '$_id.regionName',
        territory: '$_id.territory',
        territoryAbbreviation: '$_id.territoryAbbreviation',
        volunteers: 1,
      },
    },
  ]

  return await mongoCRUD.queryCollection(Regions, pipeline)
}

exports.getVolunteersByRegionAndZoneID = async function (regionID, zoneID) {
  //exports.getVolunteersByRegionAndZoneID (regionID, zoneID) {
  const pipeline = [
    {
      $match: {
        regionID: regionID,
      },
    },
    {
      $lookup: {
        from: 'zones',
        localField: 'regionName',
        foreignField: 'zoneRegion',
        as: 'zones',
      },
    },
    {
      $unwind: {
        path: '$zones',
      },
    },
    {
      $match: {
        'zones.zoneID': zoneID,
      },
    },
    {
      $lookup: {
        from: 'congregations',
        localField: 'zones.zoneID',
        foreignField: 'zones.zoneID',
        as: 'congregations',
      },
    },
    {
      $unwind: {
        path: '$congregations',
      },
    },
    {
      $lookup: {
        from: 'users',
        localField: 'congregations.congregationID',
        foreignField: 'congregationID',
        as: 'volunteers',
      },
    },
    {
      $unwind: {
        path: '$volunteers',
      },
    },
    {
      $group: {
        _id: {
          regionID: '$regionID',
          regionName: '$regionName',
          territory: '$territories.name',
          territoryAbbreviation: '$territories.abbreviation',
        },
        volunteers: {
          $addToSet: {
            userID: '$volunteers.userID',
            firstName: '$volunteers.firstName',
            lastName: '$volunteers.lastName',
          },
        },
      },
    },
    {
      $project: {
        _id: 0,
        regionID: '$_id.regionID',
        regionName: '$_id.regionName',
        territory: '$_id.territory',
        territoryAbbreviation: '$_id.territoryAbbreviation',
        volunteers: 1,
      },
    },
  ]

  return await mongoCRUD.queryCollection(Regions, pipeline)
}

exports.getVolunteersByRegionAndTerritoryID = async function (
  regionID,
  territoryID,
) {
  //exports.getVolunteersByRegionAndTerritoryID (regionID, territoryID) {
  const pipeline = [
    {
      $match: {
        regionID: regionID,
      },
    },
    {
      $lookup: {
        from: 'territories',
        localField: 'regionName',
        foreignField: 'territoryRegion',
        as: 'territories',
      },
    },
    {
      $unwind: {
        path: '$territories',
      },
    },
    {
      $match: {
        'territories.territoryID': territoryID,
      },
    },
    {
      $unwind: {
        path: '$territories.zones',
        preserveNullAndEmptyArrays: false,
      },
    },
    {
      $lookup: {
        from: 'congregations',
        localField: 'territories.zones.zoneID',
        foreignField: 'zones.zoneID',
        as: 'congregations',
      },
    },
    {
      $unwind: {
        path: '$congregations',
      },
    },
    {
      $lookup: {
        from: 'users',
        localField: 'congregations.congregationID',
        foreignField: 'congregationID',
        as: 'volunteers',
      },
    },
    {
      $unwind: {
        path: '$volunteers',
      },
    },
    {
      $group: {
        _id: {
          regionID: '$regionID',
          regionName: '$regionName',
          territory: '$territories.name',
          territoryAbbreviation: '$territories.abbreviation',
        },
        volunteers: {
          $addToSet: {
            userID: '$volunteers.userID',
            firstName: '$volunteers.firstName',
            lastName: '$volunteers.lastName',
          },
        },
      },
    },
    {
      $project: {
        _id: 0,
        regionID: '$_id.regionID',
        regionName: '$_id.regionName',
        territory: '$_id.territory',
        territoryAbbreviation: '$_id.territoryAbbreviation',
        volunteers: 1,
      },
    },
  ]

  return await mongoCRUD.queryCollection(Regions, pipeline)
}

const preprocessCriterion = async (user, criterion) => {
  if (hasNestedCriterion(VOLUNTEER_STAGES.ZONE, criterion)) {
    let zoneCriterion = {}
    if ('primaryContactId' in criterion[VOLUNTEER_STAGES.ZONE]) {
      zoneCriterion.primaryContactId =
        criterion[VOLUNTEER_STAGES.ZONE].primaryContactId
      delete criterion[VOLUNTEER_STAGES.ZONE].primaryContactId
    }
    if ('assistantContactId' in criterion[VOLUNTEER_STAGES.ZONE]) {
      zoneCriterion.assistantContactId =
        criterion[VOLUNTEER_STAGES.ZONE].assistantContactId
      delete criterion[VOLUNTEER_STAGES.ZONE].assistantContactId
    }
    if ('contactId' in criterion[VOLUNTEER_STAGES.ZONE]) {
      zoneCriterion.contactId = criterion[VOLUNTEER_STAGES.ZONE].contactId
      delete criterion[VOLUNTEER_STAGES.ZONE].contactId
    }
    if (Object.keys(zoneCriterion).length > 0) {
      const Zones = require('../zones/zones.controller')
      const zoneIds = (
        await Zones.getZones({
          user,
          criterion: zoneCriterion,
          columns: ['zoneID'],
          returned: { data: true, count: false },
        })
      ).data.map((z) => z.zoneID)
      criterion[VOLUNTEER_STAGES.ZONE].zoneID =
        'zoneID' in criterion[VOLUNTEER_STAGES.ZONE]
          ? (Array.isArray(criterion[VOLUNTEER_STAGES.ZONE].zoneID)
              ? criterion[VOLUNTEER_STAGES.ZONE].zoneID
              : [criterion[VOLUNTEER_STAGES.ZONE].zoneID]
            ).filter((z) => zoneIds.includes(z))
          : zoneIds
    }
  }
  return criterion
}

const getVolunteerBasicCriterion = ({
  doIt,
  criterion,
  columns,
  sort,
  pipeline,
  // eslint-disable-next-line no-unused-vars
  suppress_contact_restrictions,
}) => {
  if (doIt) {
    let matches = []
    for (const p of [
      ['username', 'username', false],
      ['email', 'email', false],
      ['userID', 'userID', false],
      ['firstName', 'firstName', true],
      ['lastName', 'lastName', true],
      ['city', 'city', true],
      ['state', 'state', false],
      ['status', 'status', false],
      ['role', 'role', false],
      ['volunteerGender', 'volunteerGender', false],
      ['congregationID', 'congregationID', false],
      ['languageID', 'language.languageID', false],
      ['languageProficiency', 'language.proficiency', false],
      ['maritalStatus', 'maritalStatus', false],
      ['deleted', 'deleted', false],
      ['isAllowedInterest', 'isAllowedInterest', false],
      ['isICLWVolunteer', 'isICLWVolunteer', false],
      ['isICLWContact', 'isICLWContact', false],
      ['applicantID', 'applicantID', false],
      ['appointmentResponsibility', 'appointment.responsibility', false],
      ['facilityID', 'facilities.facilityID', false],
      ['facilityGender', 'facilities.gender', false],
      ['deleted', 'deleted', false],
      ['facilityAssignmentContact', 'facilities.assignment.contact', false],
      [
        'facilityAssignmentCorrespondence',
        'facilities.assignment.correspondence',
        false,
      ],
      [
        'facilityAssignmentInPerson',
        'facilities.assignment.inPersonVisits',
        false,
      ],
      ['facilityAssignmentIclw', 'facilities.assignment.iclw', false],
      ['facilityAssignmentMeetings', 'facilities.assignment.meetings', false],
      ['isAdmin', 'isAdmin', false],
      ['isBranchRep', 'isBranchRep', false],
      ['gender', 'gender', false],
    ]) {
      matches = addMatch({
        criterion,
        criterionProperty: p[0],
        targetProperty: p[1],
        doRegex: p[2],
        matches,
      })
    }
    if (
      'region' in criterion &&
      !hasNestedCriterion(VOLUNTEER_STAGES.REGION, criterion)
    ) {
      matches = addMatch({
        criterion,
        criterionProperty: 'region',
        matches,
      })
    }
    if ('startBirthDate' in criterion) {
      matches.push({ birthDate: { $gte: criterion.startBirthDate } })
    }
    if ('endBirthDate' in criterion) {
      matches.push({ birthDate: { $lte: criterion.endBirthDate } })
    }
    if ('startBaptismDate' in criterion) {
      matches.push({ baptismDate: { $gte: criterion.startBaptismDate } })
    }
    if ('endBaptismDate' in criterion) {
      matches.push({ baptismDate: { $lte: criterion.endBaptismDate } })
    }
    if ('startBadgeExpirationDate' in criterion) {
      matches.push({
        'facilities.badgeExpiration': {
          $gte: criterion.startBadgeExpirationDate,
        },
      })
    }
    if ('endBadgeExpirationDate' in criterion) {
      matches.push({
        'facilities.badgeExpiration': {
          $gte: criterion.endBadgeExpirationDate,
        },
      })
    }
    if (!('deleted' in criterion)) {
      matches.push({ deleted: { $ne: true } })
    }
    pipeline = pipeline.concat(
      createAddFields({
        stage: VOLUNTEER_STAGES.VOLUNTEER,
        criterion,
        columns,
        sort,
        columnMap,
      }),
    )
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteerFacilityCriterion = ({
  doIt,
  criterion,
  columns,
  sort,
  pipeline,
}) => {
  if (doIt) {
    let matches = []
    if (hasNestedCriterion(VOLUNTEER_STAGES.FACILITIES, criterion)) {
      for (const p of [
        ['locationName', 'locationName', true],
        ['state', 'state', false],
        ['type', 'type', false],
        ['gender', 'facilityGender', false],
        ['agencyType', 'agencyType', false],
        ['region', 'region', false],
        ['externalID', 'externalID', false],
        ['primaryContactId', 'overseer', false],
        ['assistantContactId', 'assistantContacts', false],
      ]) {
        matches = addMatch({
          criterion,
          criterionProperty: p[0],
          targetProperty: `${VOLUNTEER_STAGES.FACILITIES}.${p[1]}`,
          doRegex: p[2],
          matches,
        })
      }
    }
    pipeline = pipeline.concat([
      {
        $lookup: {
          from: 'facilities',
          localField: 'facilities.facilityID',
          foreignField: 'facilityID',
          as: 'facilitiess',
        },
      },
      {
        $addFields: {
          facilitiess: {
            $map: {
              input: '$facilitiess',
              as: 'f',
              in: {
                $mergeObjects: [
                  '$$f',
                  {
                    facilityGender: '$$f.gender',
                  },
                ],
              },
            },
          },
        },
      },
      {
        $project: {
          'facilitiess.gender': 0,
        },
      },
      {
        $addFields: {
          facilities: {
            $map: {
              input: '$facilities',
              as: 'f',
              in: {
                $mergeObjects: [
                  '$$f',
                  {
                    $arrayElemAt: [
                      {
                        $filter: {
                          input: '$facilitiess',
                          as: 'fa',
                          cond: { $eq: ['$$fa.facilityID', '$$f.facilityID'] },
                        },
                      },
                      0,
                    ],
                  },
                ],
              },
            },
          },
        },
      },
      {
        $project: {
          facilitiess: 0,
          'facilities._id': 0,
        },
      },
    ])
    pipeline = pipeline.concat(
      createAddFields({
        stage: VOLUNTEER_STAGES.FACILITIES,
        criterion,
        columns,
        sort,
        columnMap,
      }),
    )
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteerFacilityPrimaryContactCriterion = ({
  doIt,
  criterion,
  columns,
  sort,
  pipeline,
}) => {
  if (doIt) {
    let matches = []
    if (
      hasNestedCriterion(VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT, criterion)
    ) {
      for (const p of ['userID', 'email']) {
        matches = addMatch({
          criterion: criterion[VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT],
          criterionProperty: p,
          targetProperty: `${VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT}.${p}`,
          matches,
        })
      }
    }
    pipeline = pipeline.concat([
      {
        $lookup: {
          from: 'users',
          localField: 'facilities.overseer',
          foreignField: 'email',
          as: VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT,
        },
      },
    ])
    pipeline = pipeline.concat(
      createAddFields({
        stage: VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT,
        criterion,
        columns,
        sort,
        columnMap,
      }),
    )
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteerCongregationCriterion = ({
  doIt,
  criterion,
  columns,
  sort,
  pipeline,
}) => {
  if (doIt) {
    let matches = []
    if (hasNestedCriterion(VOLUNTEER_STAGES.CONGREGATION, criterion)) {
      for (const p of [
        ['circuitID', false],
        ['zoneID', false],
        ['congregationName', true],
        ['congregationNumber', false],
        ['languageID', false],
      ]) {
        matches = addMatch({
          criterion: criterion[VOLUNTEER_STAGES.CONGREGATION],
          criterionProperty: p,
          targetProperty: `${VOLUNTEER_STAGES.CONGREGATION}.${p}`,
          matches,
        })
      }
    }
    pipeline = pipeline.concat([
      {
        $lookup: {
          from: 'congregations',
          localField: 'congregationID',
          foreignField: 'congregationID',
          as: VOLUNTEER_STAGES.CONGREGATION,
        },
      },
      {
        $unwind: {
          path: `$${VOLUNTEER_STAGES.CONGREGATION}`,
          preserveNullAndEmptyArrays: true,
        },
      },
    ])
    pipeline = pipeline.concat(
      createAddFields({
        stage: VOLUNTEER_STAGES.CONGREGATION,
        criterion,
        columns,
        sort,
        columnMap,
      }),
    )
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteerZoneCriterion = ({
  doIt,
  criterion,
  columns,
  sort,
  pipeline,
}) => {
  if (doIt) {
    let matches = []
    if (hasNestedCriterion(VOLUNTEER_STAGES.ZONE, criterion)) {
      for (const p of [
        ['zoneID', false],
        ['zoneName', true],
      ]) {
        matches = addMatch({
          criterion: criterion[VOLUNTEER_STAGES.ZONE],
          criterionProperty: p[0],
          targetProperty: `${VOLUNTEER_STAGES.ZONE}.${p[0]}`,
          doRegex: p[1],
          matches,
        })
      }
    }
    pipeline = pipeline.concat([
      {
        $lookup: {
          from: 'zones',
          localField: 'congregation.zones.zoneID',
          foreignField: 'zoneID',
          as: VOLUNTEER_STAGES.ZONE,
        },
      },
    ])
    pipeline = pipeline.concat(
      createAddFields({
        stage: VOLUNTEER_STAGES.ZONE,
        criterion,
        columns,
        sort,
        columnMap,
      }),
    )
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteerContactZoneCriterion = ({
  doIt,
  criterion,
  columns,
  sort,
  pipeline,
}) => {
  if (doIt) {
    let matches = []
    if (VOLUNTEER_STAGES.CONTACT_ZONES in criterion) {
      if ('zoneID' in criterion[VOLUNTEER_STAGES.CONTACT_ZONES]) {
        matches.push({
          $or: [
            createMatch({
              criterion: criterion[VOLUNTEER_STAGES.CONTACT_ZONES],
              criterionProperty: 'zoneID',
              targetProperty: `${VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES}.zoneID`,
            }),
            createMatch({
              criterion: criterion[VOLUNTEER_STAGES.CONTACT_ZONES],
              criterionProperty: 'zoneID',
              targetProperty: `${VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES}.zoneID`,
            }),
            // TODO Remove when on V4
            createMatch({
              criterion: criterion[VOLUNTEER_STAGES.CONTACT_ZONES],
              criterionProperty: 'zoneID',
              targetProperty: `${VOLUNTEER_STAGES.V3_CONTACT_ZONES}.zoneID`,
            }),
          ],
        })
      }
    }
    pipeline = pipeline.concat([
      {
        $lookup: {
          from: 'zones',
          localField: 'userID',
          foreignField: 'contactID',
          as: VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES,
        },
      },
      {
        $unwind: {
          path: `$${VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES}`,
          preserveNullAndEmptyArrays: true,
        },
      },
      {
        $lookup: {
          from: 'zones',
          localField: 'userID',
          foreignField: 'assistantContacts',
          as: VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES,
        },
      },
      {
        $lookup: {
          from: 'zones',
          localField: 'userID',
          foreignField: 'zoneContacts.userID',
          as: VOLUNTEER_STAGES.V3_CONTACT_ZONES,
        },
      },
    ])
    pipeline = pipeline.concat(
      createAddFields({
        stage: VOLUNTEER_STAGES.CONTACT_ZONES,
        criterion,
        columns,
        sort,
        columnMap,
      }),
    )
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteerRegionCriterion = ({
  doIt,
  criterion,
  columns,
  sort,
  pipeline,
}) => {
  if (doIt) {
    let matches = []
    if (hasNestedCriterion(VOLUNTEER_STAGES.REGION, criterion)) {
      for (const p of [
        ['regionID', 'regionID'],
        ['primaryContactId', 'contactID'],
        ['assistantContactId', 'assistantContacts'],
      ]) {
        matches = addMatch({
          criterion: criterion[VOLUNTEER_STAGES.REGION],
          criterionProperty: p[0],
          targetProperty: `${VOLUNTEER_STAGES.REGION}.${p[1]}`,
          matches,
        })
      }
    }
    pipeline = pipeline.concat([
      {
        $lookup: {
          from: 'regions',
          localField: 'region',
          foreignField: 'region',
          as: VOLUNTEER_STAGES.REGION,
        },
      },
      {
        $unwind: {
          path: VOLUNTEER_STAGES.REGION,
          preserveNullAndEmptyArrays: true,
        },
      },
    ])
    pipeline = pipeline.concat(
      createAddFields({
        stage: VOLUNTEER_STAGES.REGION,
        criterion,
        columns,
        sort,
        columnMap,
      }),
    )
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteerContactRegionCriterion = ({
  doIt,
  criterion,
  columns,
  sort,
  pipeline,
}) => {
  if (doIt) {
    let matches = []
    if (VOLUNTEER_STAGES.CONTACT_REGIONS in criterion) {
      if ('regionID' in criterion[VOLUNTEER_STAGES.CONTACT_REGIONS]) {
        matches.push({
          $or: [
            createMatch({
              criterion: criterion[VOLUNTEER_STAGES.CONTACT_REGIONS],
              criterionProperty: 'regionID',
              targetProperty: `${VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS}.regionID`,
            }),
            createMatch({
              criterion: criterion[VOLUNTEER_STAGES.CONTACT_REGIONS],
              criterionProperty: 'regionID',
              targetProperty: `${VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS}.regionID`,
            }),
          ],
        })
      }
    }
    pipeline = pipeline.concat([
      {
        $lookup: {
          from: 'regions',
          localField: 'userID',
          foreignField: 'contactID',
          as: VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS,
        },
      },
      {
        $unwind: {
          path: `$${VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS}`,
          preserveNullAndEmptyArrays: true,
        },
      },
      {
        $lookup: {
          from: 'regions',
          localField: 'userID',
          foreignField: 'assistantContacts',
          as: VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS,
        },
      },
    ])
    pipeline = pipeline.concat(
      createAddFields({
        stage: VOLUNTEER_STAGES.CONTACT_REGIONS,
        criterion,
        columns,
        sort,
        columnMap,
      }),
    )
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteerSecurityModuleCriterion = ({
  doIt,
  module,
  criterion,
  // eslint-disable-next-line no-unused-vars
  columns,
  // eslint-disable-next-line no-unused-vars
  sort,
  pipeline,
}) => {
  if (doIt) {
    let matches = []
    const key = `${VOLUNTEER_STAGES.SECURITY}${module}`
    if (hasNestedCriterion(key, criterion)) {
      for (const p of ['module', 'level', 'access']) {
        matches = addMatch({
          criterion: criterion[key],
          criterionProperty: p,
          targetProperty: `${key}.${p}`,
          matches,
        })
      }
    }
    pipeline = pipeline.concat([
      {
        $addFields: {
          [key]: {
            $filter: {
              input: '$security',
              as: 'sec',
              cond: { $eq: ['$$sec.module', module] },
            },
          },
        },
      },
      {
        $unwind: {
          path: `$${key}`,
          preserveNullAndEmptyArrays: true,
        },
      },
    ])
    if (matches.length > 0) {
      pipeline = pipeline.concat([{ $match: { $and: matches } }])
    }
  }
  return pipeline
}

const getVolunteers = async ({
  user,
  criterion = {},
  pagination,
  columns,
  sort,
  dropColumns,
  // eslint-disable-next-line no-unused-vars
  insensitive,
  returned = DEFAULT_RETURNED,
  remapKey = null,
  suppress_contact_restrictions = false,
}) => {
  const hasFacilityProperties = areStageColumnsProvided(
    `${VOLUNTEER_STAGES.FACILITIES}.`,
    columns,
  )
  criterion = await preprocessCriterion(user, criterion)
  let pipeline = getVolunteerBasicCriterion({
    doIt: true,
    criterion,
    columns,
    sort,
    pipeline: [],
    suppress_contact_restrictions,
  })
  pipeline = getVolunteerFacilityCriterion({
    doIt:
      hasNestedCriterion(VOLUNTEER_STAGES.FACILITIES, criterion) ||
      hasNestedCriterion(
        VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT,
        criterion,
      ) ||
      hasFacilityProperties ||
      areStageColumnsProvided(
        `${VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT}.`,
        columns,
      ),
    criterion,
    columns,
    sort,
    pipeline,
  })
  pipeline = getVolunteerFacilityPrimaryContactCriterion({
    doIt:
      hasNestedCriterion(
        VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT,
        criterion,
      ) ||
      areStageColumnsProvided(
        `${VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT}.`,
        columns,
      ),
    criterion,
    columns,
    sort,
    pipeline,
  })
  pipeline = getVolunteerCongregationCriterion({
    doIt:
      hasNestedCriterion(VOLUNTEER_STAGES.CONGREGATION, criterion) ||
      hasNestedCriterion(VOLUNTEER_STAGES.ZONE, criterion) ||
      areStageColumnsProvided(`${VOLUNTEER_STAGES.CONGREGATION}.`, columns) ||
      areStageColumnsProvided(`${VOLUNTEER_STAGES.ZONE}.`, columns),
    criterion,
    columns,
    sort,
    pipeline,
  })
  pipeline = getVolunteerRegionCriterion({
    doIt:
      hasNestedCriterion(VOLUNTEER_STAGES.REGION, criterion) ||
      areStageColumnsProvided(`${VOLUNTEER_STAGES.REGION}.`, columns),
    criterion,
    pipeline,
  })
  pipeline = getVolunteerContactRegionCriterion({
    doIt:
      hasNestedCriterion(VOLUNTEER_STAGES.CONTACT_REGIONS, criterion) ||
      areStageColumnsProvided(
        `${VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS}.`,
        columns,
      ) ||
      areStageColumnsProvided(
        `${VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS}.`,
        columns,
      ),
    criterion,
    columns,
    sort,
    pipeline,
  })
  pipeline = getVolunteerZoneCriterion({
    doIt:
      hasNestedCriterion(VOLUNTEER_STAGES.ZONE, criterion) ||
      areStageColumnsProvided(`${VOLUNTEER_STAGES.ZONE}.`, columns),
    criterion,
    columns,
    sort,
    pipeline,
  })
  pipeline = getVolunteerContactZoneCriterion({
    doIt:
      hasNestedCriterion(VOLUNTEER_STAGES.CONTACT_ZONES, criterion) ||
      areStageColumnsProvided(
        `${VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES}.`,
        columns,
      ) ||
      areStageColumnsProvided(
        `${VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES}.`,
        columns,
      ) ||
      // TODO Remove when on v4
      areStageColumnsProvided(`${VOLUNTEER_STAGES.V3_CONTACT_ZONES}.`, columns),
    criterion,
    columns,
    sort,
    pipeline,
  })
  pipeline = getVolunteerSecurityModuleCriterion({
    doIt:
      hasNestedCriterion(
        `${VOLUNTEER_STAGES.SECURITY}${MODULE_FACILITIES}`,
        criterion,
      ) ||
      areStageColumnsProvided(
        `${VOLUNTEER_STAGES.SECURITY}${MODULE_FACILITIES}.`,
        columns,
      ),
    module: MODULE_FACILITIES,
    criterion,
    columns,
    sort,
    pipeline,
  })

  if (criterion.searchTerm) {
    pipeline = pipeline.concat([
      {
        $match: {
          $or: [
            { firstName: { $regex: criterion.searchTerm, $options: 'i' } },
            { lastName: { $regex: criterion.searchTerm, $options: 'i' } },
            { email: { $regex: criterion.searchTerm, $options: 'i' } },
          ],
        },
      },
    ])
  }

  const countPipeline = buildCount(pipeline)
  if (sort) {
    sort.pipelineSort = { $sort: { ...sort } }
    pipeline = pipeline.concat(
      sort.pipelineSort,
      buildProjection({ columns, dropColumns }),
      buildPagination(pagination),
    )
  }

  const volunteers = await buildResponse({
    returned,
    dataPromise: returned.data
      ? mongoCRUD.queryCollection(Volunteer, pipeline)
      : null,
    totalPromise: returned.total
      ? mongoCRUD.queryCollection(Volunteer, countPipeline)
      : null,
    remapKey,
  })
  if (!columns || columns.includes('photoLink')) {
    for (let v of volunteers.data) {
      v.photoLink = (
        await s3Connection.getPhotoSignedURL(user.region, v.photoLink)
      ).data
    }
  }
  return volunteers
}
exports.getVolunteers = getVolunteers

const getVolunteer = async function ({ user, userId, columns }) {
  const volunteers = await getVolunteers({
    user,
    criterion: { userID: userId },
    columns,
  })
  if (volunteers.total === 0) {
    return { data: null }
  }
  let volunteer = { ...volunteerPropsDefaults, ...volunteers.data[0] }
  if (columns) {
    volunteer = columns.reduce((i, c) => {
      const k = c.split('.')[0]
      return { ...i, [k]: volunteer[k] }
    }, {})
  }
  /*
    if ('region' in volunteer) {
      const regionPrimaryContact = await RegionController.getPrimaryRegionContacts(volunteer.region)
      if (regionPrimaryContact.length > 0) {
        volunteer.primaryRegionContacts = regionPrimaryContact
      }
      const regionAssistantContact = await RegionController.getAssistantRegionContacts(volunteer.region)
      if (regionAssistantContact.length > 0) {
        volunteer.assistantRegionContacts = regionAssistantContact
      }
    }
    if (volunteer.congregation && 'zones' in volunteer.congregation) {
      const zonePrimaryContact = await ZoneController.getPrimaryZoneContacts(volunteer.congregation.zones)
      if (zonePrimaryContact.length > 0) {
        volunteer.primaryZoneContacts = zonePrimaryContact
      }
      const zoneAssistantContact = await ZoneController.getAssistantZoneContacts(volunteer.congregation.zones)
      if (zoneAssistantContact.length > 0) {
        volunteer.assistantZoneContacts = zoneAssistantContact
      }
    }
    */
  return { data: volunteer }
}
exports.getVolunteer = getVolunteer

const upsertVolunteer = async ({ user, volunteer }) => {
  const now = getUtcDateNow()
  let currState
  if (
    'role' in volunteer ||
    'primaryContact' in volunteer ||
    'assistantContact' in volunteer
  ) {
    currState = {
      role: volunteer.role,
      primaryContact: volunteer.primaryContact,
      assistantContact: volunteer.assistantContact,
      region: volunteer.regionName,
      zone: volunteer.zoneId,
      facilities: volunteer.facilities,
      security: volunteer.security,
    }
    delete volunteer.zoneId
    delete volunteer.regionName
    delete volunteer.primaryContact
    delete volunteer.assistantContact
  }
  if (volunteer.userID) {
    let prior
    if (currState) {
      prior = (
        await getVolunteers({
          user,
          criterion: {
            userID: volunteer.userID,
          },
          columns: [
            'role',
            'username',
            `${VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS}.regionName`,
            `${VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS}.contactID`,
            `${VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS}.regionName`,
            `${VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS}.assistantContacts`,
            `${VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES}.zoneID`,
            `${VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES}.contactID`,
            `${VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES}.zoneID`,
            `${VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES}.assistantContacts`,
            // TODO Remove when on v4
            `${VOLUNTEER_STAGES.V3_CONTACT_ZONES}.zoneID`,
            `${VOLUNTEER_STAGES.V3_CONTACT_ZONES}.assistantContacts`,
            // End remove
            'facilities',
            'security',
          ],
        })
      ).data[0]
    }
    if (volunteer.role && prior.role !== volunteer.role) {
      volunteer.security = await createSecurityArray(volunteer.role)
    }
    if (
      'isAllowedInterest' in volunteer &&
      volunteer.isAllowedInterest === false
    ) {
      volunteer.facilities = await switchInmatesAccessOff(user, volunteer)
    }
    volunteer = {
      ...volunteer,
      ...{
        modified: now,
        modifiedBy: user.userID,
      },
    }
    const {
      _doc: { _id, __v, ...result },
    } = await mongoCRUD.updateCollection(
      Volunteer,
      { userID: volunteer.userID },
      volunteer,
    )
    volunteer = result
    if (currState) {
      let prevState = {
        role: prior.role,
        region:
          prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS] &&
          prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS].regionName
            ? prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS].regionName
            : prior[VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS] &&
              prior[VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS].length > 0
            ? prior[VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS][0].regionName
            : null,
        zone:
          prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES] &&
          prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES].zoneID
            ? prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES].zoneID
            : prior[VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES] &&
              prior[VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES].length > 0
            ? prior[VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES][0].zoneID
            : // TODO Remove when on v4
            prior[VOLUNTEER_STAGES.V3_CONTACT_ZONES] &&
              prior[VOLUNTEER_STAGES.V3_CONTACT_ZONES].length > 0
            ? prior[VOLUNTEER_STAGES.V3_CONTACT_ZONES][0].zoneID
            : null,
        facilities: volunteer.facilities,
      }
      if (prevState.region) {
        prevState.primaryContact = !!(
          prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS] &&
          prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS].contactID
        )
        prevState.assistantContact = !prevState.primaryContact
      } else if (prevState.zone) {
        prevState.primaryContact = !!(
          (prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES] &&
            prior[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES].contactID) ||
          // TODO Remove V3_CONTACT_ZONES references when on v4
          (prior[VOLUNTEER_STAGES.V3_CONTACT_ZONES] &&
            prior[VOLUNTEER_STAGES.V3_CONTACT_ZONES].length > 0 &&
            prior[VOLUNTEER_STAGES.V3_CONTACT_ZONES][0].userID)
        )
        prevState.assistantContact = !prevState.primaryContact
      } else {
        prevState.primaryContact = false
        prevState.assistantContact = false
      }
      await changeRoleContact({
        user,
        userId: volunteer.userID,
        username: prior.username,
        current: currState,
        previous: prevState,
      })
    }
  } else {
    volunteer = {
      ...volunteerPropsDefaults,
      ...volunteer,
      ...{
        userID: randomUniqueIndentifier(),
        created: now,
        createdBy: user.userID,
      },
    }
    const {
      _doc: { _id, __v, ...result },
    } = await new Volunteer(volunteer).save()
    volunteer = result
    if (currState) {
      await changeRoleContact({
        user,
        userId: volunteer.userID,
        username: volunteer.username,
        current: currState,
      })
    }
  }
  return { data: volunteer }
}
exports.upsertVolunteer = upsertVolunteer

exports.getInmatesForVolunteer = async function (
  req,
  volunteer,
  inmateType,
  sort,
) {
  const inmateTypes = ['interested', 'initialCall']
  if (!inmateType.includes(inmateType)) {
    return {
      error: 'Unknown inmate type, use one of: ' + inmateTypes.join(','),
    }
  }

  const criterion = {}
  switch (inmateType) {
    case 'interested':
      // TODO: Once we figure out what this means: filter correctly
      criterion.assignedVolunteerId = volunteer.userID
      break
    case 'initialCall':
      // TODO: Once we figure out what this means: filter correctly
      criterion.assignedVolunteerId = volunteer.userID
      break
    default:
      criterion.assignedVolunteerId = volunteer.userID
  }

  const response = await InmatesController.getInmates({
    user: req.user,
    criterion,
    columns: [
      'inmateID',
      'firstName',
      'middleName',
      'lastName',
      'inmateNumber',
      'language',
      'photoLink',
      'facility.facilityID',
      'facility.locationName',
      'facility.city',
      'facility.state',
      // Interest status:
      'attendsMeeting',
      'meetingParts',
      'study',
      'specialWatch',
      'correspondence',
      'branchSubscription',
      'literatureOnly',
      'baptized',
      'disfellowshipped',
      'unbaptizedPublisher',
      'inmateStatus',
      'inactive',
    ],
    sort,
    suppress_facility_restrictions: true,
  })

  if (response.data.length > 0) {
    // Add signed url to applicant
    await s3Connection.determinePresignedURLs(
      req,
      response.data,
      'Photo',
      'photoLink',
      'photoLinkURL',
    )
  }

  return response
}

const switchInmatesAccessOff = async function (user, volunteer) {
  let userID = volunteer.userID
  let existingVolunteer = await getVolunteer({
    user,
    userId: userID,
    columns: ['role', 'username', 'facilities'],
  })
  let facilities = existingVolunteer.data.facilities
  let newAssignments
  for (let f = 0; f < facilities.length; f++) {
    newAssignments = {
      contact: facilities[f].assignments.contact,
      correspondence: false,
      inPersonVisits: false,
      iclw: facilities[f].assignments.iclw,
      meetings: false,
    }
    facilities[f].assignments = newAssignments
  }

  await Meetings.declineMeetingParts(user, userID)
  return facilities
}

exports.getVolunteerFacilities = async function (req) {
  const pipeline = [
    {
      $match: {
        userID: req.params.id,
      },
    },
    {
      $lookup: {
        from: 'congregations',
        localField: 'congregationID',
        foreignField: 'congregationID',
        as: 'congregation',
      },
    },
    {
      $lookup: {
        from: 'facilities',
        localField: 'congregation.zones.zoneID',
        foreignField: 'zoneID',
        as: 'facilities',
      },
    },
    {
      $unwind: {
        path: '$facilities',
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $match: {
        'facilities.deleted': { $ne: true },
      },
    },
    {
      $project: {
        _id: 0,
        userID: 1,
        facilityID: '$facilities.facilityID',
        locationName: '$facilities.locationName',
        city: '$facilities.city',
        state: '$facilities.state',
      },
    },
  ]

  return await mongoCRUD.queryCollection(Volunteer, pipeline)
}

exports.resetPassword = async function (req) {
  const emailer = new EmailService(req.user.username)
  let payload
  let token
  // eslint-disable-next-line no-unused-vars
  let sendTo
  // eslint-disable-next-line no-unused-vars
  let emailSubject
  // eslint-disable-next-line no-unused-vars
  let emailBody
  let url = process.env.URL

  let pipeline = [
    {
      $match: {
        userID: req.params.id,
      },
    },
    {
      $project: {
        userID: 1,
        username: 1,
        email: 1,
        password: 1,
      },
    },
  ]
  let updatedVolunteer = await mongoCRUD.queryCollection(Volunteer, pipeline)
  payload = emailer.createPayload(
    updatedVolunteer[0].userID,
    updatedVolunteer[0].email,
  )
  if (updatedVolunteer[0].password === undefined) {
    updatedVolunteer[0].password = process.env.SESSION_SECRET
  }
  token = emailer.createToken(payload, updatedVolunteer[0].password)
  const message = emailer.forgotPasswordEmail(url, payload, token)

  emailer.sendEmail({
    to: updatedVolunteer[0].email,
    subject: `Request to reset password`,
    body: message,
  })
  return updatedVolunteer
}

exports.resendWelcomeEmail = async function ({ user, userID }) {
  const emailer = new EmailService(user.username)
  let newPassword = Math.random().toString(36).slice(-8)
  let password = createHash(newPassword)
  const utcNow = getUtcDateNow()
  let volunteer = {
    userID: userID,
    password: password,
    lastWelcomeEmailSentDate: utcNow,
    lastWelcomeEmailSentByUserID: user.userID,
  }
  volunteer = await upsertVolunteer({ user, volunteer })
  const message = emailer.getNewUserEmail(volunteer.data.username, newPassword)
  emailer.sendEmail({
    to: volunteer.data.email,
    subject: `You have been registered with Gate Access`,
    body: message,
  })
  return volunteer
}

const assembleFacilities = async ({ user, volunteer }) => {
  const Inmates = require('../inmates/inmates.controller')
  const Meetings = require('../meetings/meetings.controller')
  let primary = volunteer[VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT]
    ? volunteer[VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT].reduce(
        (a, f) => [...a, ...(f.userID ? [f.userID] : [])],
        [],
      )
    : []
  let assistants = volunteer[VOLUNTEER_STAGES.FACILITIES].reduce(
    (a, f) => [...a, ...(f.assistantContacts ? f.assistantContacts : [])],
    [],
  )
  primary =
    primary.length > 0
      ? (
          await getVolunteers({
            user,
            criterion: { userID: primary },
            columns: ['userID', 'name', 'email'],
          })
        ).data.reduce((p, o) => {
          p[o.email] = { userID: o.userID, name: o.name, isPrimary: true }
          return p
        }, {})
      : []
  assistants =
    assistants.length > 0
      ? (
          await getVolunteers({
            user,
            criterion: { userID: assistants },
            columns: ['userID', 'name'],
          })
        ).data.reduce(
          (m, a) => ({
            ...m,
            [a.userID]: {
              ...a,
              ...{ userID: a.userID, name: a.name, isPrimary: false },
            },
          }),
          {},
        )
      : []
  const facilities = volunteer[VOLUNTEER_STAGES.FACILITIES].map(
    (f) => f.facilityID,
  )
  const inperson = (
    await Inmates.getInmates({
      user,
      criterion: {
        inPersonVolunteerId: volunteer.userID,
        facilityID: facilities,
      },
      columns: ['facilityID', 'inmateID', 'name', 'photoLink', 'inmateNumber'],
      suppress_facility_restrictions: true,
    })
  ).data.reduce((f, i) => {
    if (i.facilityID in f) {
      f[i.facilityID].push(i)
    } else {
      f[i.facilityID] = [i]
    }
    return f
  }, {})
  const correspondence = (
    await Inmates.getInmates({
      user: user,
      criterion: {
        correspondenceVolunteerId: volunteer.userID,
        facilityID: facilities,
      },
      columns: ['facilityID', 'inmateID', 'name', 'photoLink', 'inmateNumber'],
      suppress_facility_restrictions: true,
    })
  ).data.reduce((f, i) => {
    if (i.facilityID in f) {
      f[i.facilityID].push(i)
    } else {
      f[i.facilityID] = [i]
    }
    return f
  }, {})
  const meetings = (
    await Meetings.getMeetings({
      user: user,
      criterion: {
        startMeetingDate: getUtcDateNow(),
        facilityID: facilities,
        meetingParts: {
          assignedUserID: volunteer.userID,
        },
      },
      columns: ['facilityID', 'meetingParts.assignedUserID'],
    })
  ).data.reduce((f, i) => {
    if (i.facilityID in f) {
      f[i.facilityID].push(i)
    } else {
      f[i.facilityID] = [i]
    }
    return f
  }, {})

  volunteer.isCorrespondingWithInmate =
    correspondence && Object.keys(correspondence).length > 0
  volunteer.isInPersonWithInmate = inperson && Object.keys(inperson).length > 0
  volunteer.isFutureMeetingAssignments =
    meetings && Object.keys(meetings).length > 0
  const inmates = []
    .concat(...Object.values(correspondence))
    .concat(...Object.values(inperson))
    .reduce((a, i) => {
      if (i.facilityID in a) {
        if (
          a[i.facilityID].filter((f) => f.inmateID === i.inmateID).length === 0
        ) {
          a[i.facilityID].push({
            inmateID: i.inmateID,
            name: i.name,
            photoLink: i.photoLink,
            inmateNumber: i.inmateNumber,
          })
        }
      } else {
        a[i.facilityID] = [
          {
            inmateID: i.inmateID,
            name: i.name,
            photoLink: i.photoLink,
            inmateNumber: i.inmateNumber,
          },
        ]
      }
      return a
    }, {})
  for (let f of volunteer[VOLUNTEER_STAGES.FACILITIES]) {
    let contacts = []
    if (f.overseer && f.overseer in primary) {
      contacts.push(primary[f.overseer])
      f.isPrimary = primary[f.overseer].userID === volunteer.userID
    } else {
      f.isPrimary = false
    }
    if (f.assistantContacts) {
      for (const a of Object.keys(assistants)) {
        if (f.assistantContacts.includes(a)) {
          contacts.push(assistants[a])
        }
      }
      f.isAssistant = volunteer.userID in assistants
    }
    delete f.overseer
    delete f.assistantContacts
    f.contacts = contacts
    f.inmates = f.facilityID in inmates ? inmates[f.facilityID] : []
    f.isCorrespondingWithInmate = f.facilityID in correspondence
    f.isInPersonWithInmate = f.facilityID in inperson
    f.isFutureMeetingAssignments = f.facilityID in meetings
  }
  return volunteer
}

exports.getAccess = async ({ user, userId, columns }) => {
  let volunteer = await getVolunteers({
    user: user,
    criterion: {
      userID: userId,
    },
    columns,
  })
  if (volunteer.total == 0) {
    return null
  }
  volunteer = volunteer.data[0]
  volunteer.region =
    volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS] &&
    volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS].regionName
      ? volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS].regionName
      : volunteer[VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS] &&
        volunteer[VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS].length > 0
      ? volunteer[VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS][0].regionName
      : null
  volunteer.zone =
    volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES] &&
    volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES].zoneID
      ? volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES].zoneID
      : volunteer[VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES] &&
        volunteer[VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES].length > 0
      ? volunteer[VOLUNTEER_STAGES.ASSISTANT_CONTACT_ZONES][0].zoneID
      : null
  if (volunteer.region) {
    volunteer.primaryContact =
      !!volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS]
    volunteer.assistantContact = !volunteer.primaryContact
  } else if (volunteer.zone) {
    volunteer.primaryContact =
      !!volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES]
    volunteer.assistantContact = !volunteer.primaryContact
  } else {
    volunteer.primaryContact = false
    volunteer.assistantContact = false
  }
  volunteer = await assembleFacilities({ user, volunteer })
  delete volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_REGIONS]
  delete volunteer[VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS]
  delete volunteer[VOLUNTEER_STAGES.PRIMARY_CONTACT_ZONES]
  delete volunteer[VOLUNTEER_STAGES.ASSISTANT_CONTACT_REGIONS]
  delete volunteer[VOLUNTEER_STAGES.V3_CONTACT_ZONES]
  delete volunteer[VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT]
  return { data: volunteer }
}

const changeRoleContact = async ({
  user,
  userId,
  username,
  previous,
  current,
}) => {
  const Regions = require('../regions/regions.controller')
  const Zones = require('../zones/zones.controller')
  const Facilities = require('../facilities/facilities.controller')
  if (
    previous &&
    (current.role !== previous.role ||
      current.primaryContact !== previous.primaryContact ||
      current.assistantContact !== previous.assistantContact ||
      current.zone !== previous.zone ||
      current.region !== previous.region)
  ) {
    if (previous.role === VOLUNTEER_ROLE_REGION && !!previous.region) {
      await Regions.removeContact({
        user,
        regionName: previous.region,
        userId: userId,
      })
    } else if (previous.role === VOLUNTEER_ROLE_ZONE && !!previous.zone) {
      await Zones.removeContact({
        user,
        zoneId: previous.zone,
        userId: userId,
      })
    } else if (
      previous.role === VOLUNTEER_ROLE_FACILITY &&
      current.role !== VOLUNTEER_ROLE_FACILITY
    ) {
      await Facilities.removeContact({
        user,
        username: username,
        userId: userId,
      })
      const uncheck = (
        await getVolunteer({
          user,
          userId,
          columns: ['facilities'],
        })
      ).data
      if (uncheck) {
        uncheck.facilities.forEach((f) => (f.assignments.contact = false))
        await upsertVolunteer({
          user,
          volunteer: {
            userID: userId,
            facilities: uncheck.facilities,
          },
        })
      }
    }
  }
  if (current.role === VOLUNTEER_ROLE_REGION && !!current.region) {
    await Regions.addContact({
      user,
      regionName: current.region,
      userId: userId,
      primary: current.primaryContact,
    })
  } else if (current.role === VOLUNTEER_ROLE_ZONE && !!current.zone) {
    await Zones.addContact({
      user,
      zoneId: current.zone,
      userId: userId,
      primary: current.primaryContact,
    })
  }
}

exports.getAccessFacility = async ({ user, userId, facilityId, columns }) => {
  const Facilities = require('../facilities/facilities.controller')
  const volunteer = (
    await getVolunteer({
      user,
      userId,
      columns,
    })
  ).data
  const vcolumns = columns.filter((c) => c.indexOf('.') === -1)
  let facilities = volunteer[VOLUNTEER_STAGES.FACILITIES].filter(
    (f) => f.facilityID === facilityId,
  )
  if (facilities.length > 0) {
    let v = vcolumns.reduce((f, k) => ({ ...f, [k]: volunteer[k] }), {})
    v[VOLUNTEER_STAGES.FACILITIES] = [facilities[0]]
    v[VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT] = volunteer[
      VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT
    ].filter((c) => c.email === facilities[0].overseer)
    const dataWithFacilities = await assembleFacilities({ user, volunteer: v })
    delete dataWithFacilities[VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT]
    return { data: await augmentFacilitiesWithZoneContacts(dataWithFacilities) }
  }
  const fcolumns = columns
    .filter(
      (c) =>
        c.indexOf('.') > -1 &&
        c.substring(c.indexOf('.') + 1).indexOf('.') === -1,
    )
    .map((c) => c.substring(c.indexOf('.') + 1))
  const acolumns = columns
    .filter(
      (c) =>
        c.indexOf('.') > -1 &&
        c.substring(c.indexOf('.') + 1).indexOf('.') > -1,
    )
    .map((c) => c.substring(c.indexOf('.') + 1))
    .map((c) => c.substring(c.indexOf('.') + 1))
  facilities = (
    await Facilities.getFacilities({
      user,
      criterion: {
        facilityID: facilityId,
      },
      columns: [
        'facilityID',
        'locationName',
        'city',
        'state',
        'gender',
        'overseer',
        'assistantContacts',
      ],
    })
  ).data[0]
  let v = vcolumns.reduce((f, k) => ({ ...f, [k]: volunteer[k] }), {})
  v[VOLUNTEER_STAGES.FACILITIES] = [
    fcolumns.reduce((f, k) => ({ ...f, [`${k}`]: facilities[k] }), {}),
  ]
  v[VOLUNTEER_STAGES.FACILITIES][0]['assignments'] = acolumns.reduce(
    (f, k) => ({ ...f, [`${k}`]: false }),
    {},
  )
  const dataWithFacilities = await assembleFacilities({ user, volunteer: v })
  delete dataWithFacilities[VOLUNTEER_STAGES.FACILITY_PRIMARY_CONTACT]
  return { data: await augmentFacilitiesWithZoneContacts(dataWithFacilities) }
}

/**
 * Augments the facilities array in the given data structure with each Facility's Zone's Contacts
 * @param data
 * @returns {Promise<*>}
 */
const augmentFacilitiesWithZoneContacts = async (data) => {
  const Zones = require('../zones/zones.controller')
  const facilities = data.facilities
  if (!facilities || facilities.length === 0) return data
  const zoneMatchQuery = {
    $match: { zoneID: { $in: facilities.map((fac) => fac.zoneID) } },
  }
  const [primaryZoneContacts, assistantZoneContacts] = await Promise.all([
    Zones.getPrimaryZoneContacts(zoneMatchQuery),
    Zones.getAssistantZoneContacts(zoneMatchQuery),
  ])
  facilities.forEach((f) => {
    const { zoneID } = f
    if (!zoneID) return // Shouldn't happen, but would create weird bugs if it did
    f.zoneContacts = [
      ...(!primaryZoneContacts
        ? []
        : primaryZoneContacts.map((pzc) => ({ ...pzc, isPrimary: true }))),
      ...(!assistantZoneContacts
        ? []
        : assistantZoneContacts.map((azc) => ({ ...azc, isPrimary: false }))),
    ]
      // Only include when user info is actually there, since it will also generate empty records
      // for Zones without a Primary or Assistant contact:
      .filter((zc) => Boolean(zc.userID))
      // And keep only user information
      .map((zc) => ({
        isPrimary: zc.isPrimary,
        email: zc.email,
        firstName: zc.firstName,
        lastName: zc.lastName,
        userID: zc.userID,
      }))
  })
  return data
}

const removeAccessFacility = async ({ user, volunteer, facilityId }) => {
  const Inmates = require('../inmates/inmates.controller')
  const Facilities = require('../facilities/facilities.controller')
  const f = volunteer.facilities.filter((f) => f.facilityID === facilityId)
  if (f.length > 0) {
    const facility = f[0]
    volunteer.facilities = volunteer.facilities.filter(
      (f) => f.facilityID !== facilityId,
    )
    await upsertVolunteer({ user, volunteer })
    if (facility.assignments.contact) {
      await Facilities.removeContact({
        user,
        username: volunteer.username,
        userId: volunteer.userID,
        facilityId: facility.facilityID,
      })
    }
    if (facility.assignments.correspondence) {
      await Inmates.removeCorrespondent({
        user,
        userId: volunteer.userID,
        facilityId: facility.facilityID,
      })
    }
    if (facility.assignments.inPersonVisits) {
      await Inmates.removeInPersonUser({
        user,
        userId: volunteer.userID,
        facilityId: facility.facilityID,
      })
    }
  }
}

const removePrimaryContact = async ({ user, userId, facilityId }) => {
  let volunteer = (
    await getVolunteer({ user, userId, columns: ['facilities'] })
  ).data
  if (volunteer) {
    let f = volunteer.facilities.filter((f) => f.facilityID === facilityId)
    if (f.length === 1 && f[0].assignments.contact) {
      f[0].assignments.contact = false
      await upsertVolunteer({
        user,
        volunteer: { userID: userId, facilities: volunteer.facilities },
      })
    }
  }
}

const changeFacilityContacts = async ({
  user,
  volunteer,
  newFacility,
  originalFacility,
  isPrimary,
}) => {
  const Inmates = require('../inmates/inmates.controller')
  const Facilities = require('../facilities/facilities.controller')
  let fac = (
    await Facilities.getFacilities({
      user,
      criterion: {
        facilityID: newFacility.facilityID,
      },
      columns: [
        'facilityID',
        'overseer',
        'assistantContacts',
        `${FACILITIES_STAGES.PRIMARY_CONTACT}.userID`,
      ],
    })
  ).data[0]
  if (newFacility.assignments.contact) {
    let change
    if (
      isPrimary &&
      (!fac[FACILITIES_STAGES.PRIMARY_CONTACT] ||
        volunteer.userID !== fac[FACILITIES_STAGES.PRIMARY_CONTACT].userID)
    ) {
      if (
        fac[FACILITIES_STAGES.PRIMARY_CONTACT] &&
        fac[FACILITIES_STAGES.PRIMARY_CONTACT].userID
      ) {
        await removePrimaryContact({
          user,
          userId: fac[FACILITIES_STAGES.PRIMARY_CONTACT].userID,
          facilityId: newFacility.facilityID,
        })
      }
      fac.overseer = volunteer.email
      fac.assistantContacts = fac.assistantContacts.filter(
        (a) => a !== volunteer.userID,
      )
      change = true
    } else if (
      !isPrimary &&
      fac.assistantContacts.filter((a) => a === volunteer.userID).length === 0
    ) {
      if (
        fac[FACILITIES_STAGES.PRIMARY_CONTACT] &&
        fac[FACILITIES_STAGES.PRIMARY_CONTACT].userID === volunteer.userID
      ) {
        fac.overseer = null
      }
      fac.assistantContacts.push(volunteer.userID)
      change = true
    }
    if (change) {
      await Facilities.upsertFacility({
        user,
        facility: fac,
      })
    }
  } else if (
    !newFacility.assignments.contact &&
    newFacility.assignments.contact !== originalFacility.assignments.contact
  ) {
    await Facilities.removeContact({
      user,
      facilityId: newFacility.facilityID,
      username: volunteer.username,
      userId: volunteer.userID,
    })
  }
  if (
    !newFacility.assignments.correspondence &&
    originalFacility.assignments.correspondence
  ) {
    await Inmates.removeCorrespondent({
      user,
      userId: volunteer.userID,
      facilityId: newFacility.facilityID,
    })
  }
  if (
    !newFacility.assignments.inPersonVisits &&
    originalFacility.assignments.inPersonVisits
  ) {
    await Inmates.removeInPersonUser({
      user,
      userId: volunteer.userID,
      facilityId: newFacility.facilityID,
    })
  }
}

const addFacilityContacts = async ({
  user,
  volunteer,
  facility,
  isPrimary,
}) => {
  const Facilities = require('../facilities/facilities.controller')
  let fac = (
    await Facilities.getFacilities({
      user,
      criterion: {
        facilityID: facility.facilityID,
      },
      columns: [
        'facilityID',
        'overseer',
        'assistantContacts',
        `${FACILITIES_STAGES.PRIMARY_CONTACT}.userID`,
      ],
    })
  ).data[0]
  if (isPrimary) {
    if (fac.overseer) {
      await removePrimaryContact({
        user,
        userId: fac[FACILITIES_STAGES.PRIMARY_CONTACT].userID,
        facilityId: facility.facilityID,
      })
    }
    fac.overseer = volunteer.email
  } else {
    fac.assistantContacts.push(volunteer.userID)
  }
  await Facilities.upsertFacility({
    user,
    facility: fac,
  })
}

const modifyAccessFacility = async ({ user, volunteer, facility }) => {
  const primary = !!facility.isPrimary
  delete facility.isPrimary
  delete facility.isAssistant
  const f = volunteer.facilities.filter(
    (f) => f.facilityID === facility.facilityID,
  )
  let originalFacility
  if (f.length === 0) {
    volunteer.facilities.push({ ...facilityDefaults, ...facility })
  } else {
    originalFacility = f[0]
    volunteer.facilities = volunteer.facilities.filter(
      (f) => f.facilityID !== facility.facilityID,
    )
    volunteer.facilities.push({ ...facilityDefaults, ...facility })
  }
  await upsertVolunteer({ user, volunteer })
  if (originalFacility) {
    await changeFacilityContacts({
      user,
      volunteer,
      newFacility: facility,
      originalFacility,
      isPrimary: primary,
    })
  } else if (facility.assignments.contact) {
    await addFacilityContacts({
      user,
      volunteer,
      facility,
      isPrimary: primary,
    })
  }
}

const changeAccessFacility = async ({ user, userId, facility, operation }) => {
  const volunteer = (
    await getVolunteer({
      user,
      userId,
      columns: ['userID', 'username', 'email', 'facilities'],
    })
  ).data
  if (operation === 'D') {
    await removeAccessFacility({
      user,
      volunteer,
      facilityId: facility.facilityID,
    })
  } else if (operation === 'C') {
    await modifyAccessFacility({ user, volunteer, facility })
  }
}
exports.changeAccessFacility = changeAccessFacility

if (process.env.NODE_ENV !== 'production') {
  // eslint-disable-next-line no-unused-vars
  exports.deleteVolunteer = async ({ user, criterion }) => {
    let deleted
    if ('userID' in criterion) {
      deleted = await mongoCRUD.deleteCollectionDocument(Volunteer, {
        userID: criterion.userID,
      })
    } else if ('username' in criterion) {
      deleted = await mongoCRUD.deleteCollectionDocument(Volunteer, {
        username: criterion.username,
      })
    } else {
      deleted = { deletedCount: 0 }
    }
    return { data: deleted.deletedCount }
  }
}

exports.getVolunteersByCongregation = async (req) => {
  const congregationId = sanitize(req.params.congregationId)
  const pipeline = [
    {
      $match: {
        congregationID: congregationId,
      },
    },
    {
      $count: 'total',
    },
  ]
  const result = await mongoCRUD.queryCollection(Volunteer, pipeline)
  return { count: result && result.length ? result[0].total : 0 }
}

exports.clearFacilityICLWApprovals = async (userID) => {
  try {
    await mongoCRUD.updateCollection(
      Volunteer,
      { userID },
      { $set: { 'facilities.$[].assignments.iclw': false } },
    )
  } catch (e) {
    console.error(e)
    return false
  }
  return true
}

exports.getUsersByUserName = async (userName) => {
  const pipeline = [
    {
      $match: {
        username: userName,
      },
    },
    {
      $count: 'total',
    },
  ]
  const result = await mongoCRUD.queryCollection(Volunteer, pipeline)
  return { count: result && result.length ? result[0].total : 0 }
}

exports.getVolunteerByUserId = async (userId) => {
  const pipeline = [
    {
      $match: {
        userID: userId,
      },
    },
  ]
  const result = await mongoCRUD.queryCollection(Volunteer, pipeline)
  return result[0]
}
